//! Admission and observation belong to the managed facade, not the SIP engine.
use super::*;
use crate::platform::{
    self,
    sync::{Mutex, MutexGuard},
};
use alloc::{collections::BTreeMap, sync::Weak};
use core::{future::Future, time::Duration};

type CoreDialog = crate::dialog::invite_dialog::InviteDialog;
type CoreInner = crate::dialog::dialog::DialogInner;

/// A layer-local observation ID, distinct from SIP CSeq and dialog operation IDs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PendingOperation {
    pub sequence: u64,
    pub dialog: Option<DialogId>,
    pub kind: &'static str,
    pub context: Option<String>,
    /// The observer was interrupted. Completion cannot be inferred.
    pub observation_interrupted: bool,
}

/// Owned diagnostic data; changing it cannot affect a live dialog.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DialogDiagnostics {
    pub snapshot: DialogSnapshot,
    pub persistence: Option<DialogPersistenceStatus>,
    pub session_timer: Option<SessionTimerStatus>,
}
impl DialogDiagnostics {
    pub fn waiting_ack(&self) -> bool {
        let pending = &self.snapshot.runtime.pending;
        pending.initial_ack
            || pending.remote_invite_awaiting_ack
            || self.snapshot.state == DialogSnapshotState::WaitAck
    }
}

/// A sequential observation, not an atomic snapshot of every dialog and queue.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LayerDiagnostics {
    pub admission_closed: bool,
    pub operations: Vec<PendingOperation>,
    pub dialogs: Vec<DialogDiagnostics>,
}
impl LayerDiagnostics {
    pub fn has_pending_signaling(&self) -> bool {
        !self.operations.is_empty()
            || self
                .dialogs
                .iter()
                .any(|d| !d.snapshot.runtime.pending.is_empty())
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PersistenceDrain {
    pub dialog: DialogId,
    /// None means this queue cutoff has not completed within the wait budget.
    pub result: Option<core::result::Result<(), PersistenceFailure>>,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DrainReport {
    pub timed_out: bool,
    pub remaining: LayerDiagnostics,
    pub persistence: Vec<PersistenceDrain>,
}
impl DrainReport {
    /// Completion is separate from success: inspect persistence failures too.
    pub fn is_drained(&self) -> bool {
        !self.timed_out
            && !self.remaining.has_pending_signaling()
            && self.persistence.iter().all(|p| p.result.is_some())
    }
}

pub(super) struct Control {
    admission_closed: Mutex<bool>,
    state: Mutex<ControlState>,
    publication_queues: Mutex<BTreeMap<String, Weak<super::exchange::Queue>>>,
}
struct ControlState {
    next: u64,
    operations: BTreeMap<u64, PendingOperation>,
    dialogs: BTreeMap<usize, Weak<CoreInner>>,
}
impl Control {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            admission_closed: Mutex::new(false),
            publication_queues: Mutex::new(BTreeMap::new()),
            state: Mutex::new(ControlState {
                next: 0,
                operations: BTreeMap::new(),
                dialogs: BTreeMap::new(),
            }),
        })
    }
    pub fn publication_queue(&self, resource: String) -> Arc<super::exchange::Queue> {
        let mut queues = self.publication_queues.lock();
        if let Some(queue) = queues.get(&resource).and_then(Weak::upgrade) {
            return queue;
        }
        queues.retain(|_, weak| weak.strong_count() > 0);
        let queue = Arc::new(super::exchange::Queue::new());
        queues.insert(resource, Arc::downgrade(&queue));
        queue
    }
    pub fn admit(&self) -> core::result::Result<MutexGuard<'_, bool>, AdmissionError> {
        let gate = self.admission_closed.lock();
        if *gate {
            Err(AdmissionError::LayerClosed)
        } else {
            Ok(gate)
        }
    }
    pub fn close(&self) {
        *self.admission_closed.lock() = true;
    }
    pub fn closed(&self) -> bool {
        *self.admission_closed.lock()
    }
    pub fn register(&self, dialog: &CoreDialog) {
        let mut state = self.state.lock();
        let key = Arc::as_ptr(&dialog.inner) as usize;
        if state
            .dialogs
            .get(&key)
            .is_some_and(|weak| weak.strong_count() > 0)
        {
            return;
        }
        state.dialogs.retain(|_, weak| weak.strong_count() > 0);
        state.dialogs.insert(key, Arc::downgrade(&dialog.inner));
    }
    pub fn dialogs(&self) -> Vec<CoreDialog> {
        let mut state = self.state.lock();
        state.dialogs.retain(|_, weak| weak.strong_count() > 0);
        state
            .dialogs
            .values()
            .filter_map(Weak::upgrade)
            .map(CoreDialog::from_inner)
            .collect()
    }
    pub fn track(
        self: &Arc<Self>,
        dialog: Option<DialogId>,
        kind: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> u64 {
        self.track_context(dialog, kind, None, future)
    }
    pub fn track_context(
        self: &Arc<Self>,
        dialog: Option<DialogId>,
        kind: &'static str,
        context: Option<String>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> u64 {
        let sequence = {
            let mut state = self.state.lock();
            state.next += 1;
            let sequence = state.next;
            state.operations.insert(
                sequence,
                PendingOperation {
                    sequence,
                    dialog,
                    kind,
                    context,
                    observation_interrupted: false,
                },
            );
            sequence
        };
        let guard = ObservationGuard {
            control: self.clone(),
            sequence,
            completed: false,
        };
        platform::spawn(async move {
            let mut guard = guard;
            future.await;
            guard.completed = true;
        });
        sequence
    }
    pub fn diagnostics(&self) -> LayerDiagnostics {
        let admission_closed = self.closed();
        let operations = self.state.lock().operations.values().cloned().collect();
        let dialogs = self.dialogs().iter().map(diagnose).collect();
        LayerDiagnostics {
            admission_closed,
            operations,
            dialogs,
        }
    }
    pub async fn drain(&self, budget: Duration, refresh: impl Fn()) -> DrainReport {
        self.close();
        let mut persistence = Vec::new();
        let timed_out = platform::timeout(budget, async {
            loop {
                refresh();
                if self.diagnostics().has_pending_signaling() {
                    platform::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                let dialogs = self.dialogs();
                let barriers: Vec<_> = dialogs
                    .iter()
                    .map(CoreDialog::persistence_barrier)
                    .collect();
                persistence = dialogs
                    .iter()
                    .map(|d| PersistenceDrain {
                        dialog: d.id(),
                        result: None,
                    })
                    .collect();
                for (entry, barrier) in persistence.iter_mut().zip(barriers) {
                    entry.result = Some(barrier.wait().await);
                }
                if !self.diagnostics().has_pending_signaling() {
                    break;
                }
            }
        })
        .await
        .is_err();
        refresh();
        DrainReport {
            timed_out,
            remaining: self.diagnostics(),
            persistence,
        }
    }
}
fn diagnose(dialog: &CoreDialog) -> DialogDiagnostics {
    DialogDiagnostics {
        snapshot: dialog.snapshot(),
        persistence: dialog.persistence_status(),
        session_timer: dialog.session_timer_status(),
    }
}
pub(super) fn dialog_diagnostics(dialog: &CoreDialog) -> DialogDiagnostics {
    diagnose(dialog)
}
struct ObservationGuard {
    control: Arc<Control>,
    sequence: u64,
    completed: bool,
}
impl Drop for ObservationGuard {
    fn drop(&mut self) {
        let mut state = self.control.state.lock();
        if self.completed {
            state.operations.remove(&self.sequence);
        } else if let Some(entry) = state.operations.get_mut(&self.sequence) {
            entry.observation_interrupted = true;
        }
    }
}

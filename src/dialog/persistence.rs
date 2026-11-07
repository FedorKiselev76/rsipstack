//! Opt-in durable checkpoints. Storage remains application-owned.
//!
//! Each dialog has one serial worker. A callback runs without a dialog state
//! lock and may inspect snapshots. It must not await another SIP operation on
//! the same dialog: that operation would wait behind this callback.
//!
//! Returning `Ok(())` promises the application has durably committed the supplied
//! snapshot and any related application state. No serialization or database is
//! chosen by the stack. Applications choose which message fields to serialize;
//! SIP messages may include authentication headers. Media policy remains external.
//!
//! Failure or timeout poisons this dialog's persistence worker. No subsequent
//! protected sends are permitted. A timeout cannot undo a database write that
//! has already started; recovery must inspect storage. Ordinary lifecycle events
//! are released only after the checkpoint covering them succeeds. The legacy
//! `DialogState` channel is still a protocol channel, not a durable commit feed.
//!
//! Enable this explicitly before creating or restoring calls:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use rsipstack::dialog::{dialog_layer::DialogLayer, persistence::*};
//! # fn configure(layer: &DialogLayer, store: Arc<dyn DialogPersistence>) -> rsipstack::Result<()> {
//! layer.set_persistence(Some(DialogPersistenceConfig::new(store)))?;
//! # Ok(())
//! # }
//! ```
//!
//! Protected boundaries are the first outgoing request of each dialog-owned
//! transaction (including authentication retries), an incoming final response
//! before its ACK, a locally generated final response before transport send,
//! local termination intent, and completed signaling/terminal state. A wire
//! response and its later acknowledgment are separate checkpoints. Provisional
//! responses and transaction-generated CANCEL acknowledgments do not wait for
//! storage. Pure transaction/proxy users are unaffected.
//!
//! A commit cannot be atomic with network transmission. A crash after commit
//! but before send leaves an intended message with an unknown send outcome.
//! Recovery must use the pending markers and application policy; the stack does
//! not replay in-flight transactions. A successful `accept()`/`reject()` only
//! queues a response: the transaction driver performs its checkpoint. Errors
//! from asynchronous/terminal writes are exposed by `persistence_status()`.
//! After failure, remove the dialog from the layer and reconcile stored state;
//! there is no automatic retry or fail-open mode. The application must keep its
//! async runtime alive long enough to finish accepted writes during shutdown.
//!
//! Handlers must yield rather than block an executor, and must not panic. Use
//! an application-owned incarnation key alongside dialog ID/revision for
//! snapshot idempotency across process restarts. Multiple checkpoints may share
//! a revision: it versions dialog state, not a storage operation. Deduplicate
//! message-related work with its own request/operation identity as well.
//! A new lifecycle notification is not an
//! exactly-once external side effect; persist an application outbox if needed.
//! Early checkpoints may have an empty remote tag; correlate them with the
//! later established dialog using that same application call/incarnation key.

use super::{
    dialog::{DialogInner, DialogSnapshot},
    lifecycle::DialogLifecycleEvent,
};
use crate::{
    platform::{mpsc, sync::Mutex},
    prelude::*,
    sip::SipMessage,
};
use alloc::sync::Weak;
use async_trait::async_trait;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DialogCheckpointReason {
    /// An application explicitly changed the remote target through its setter.
    TargetChanged,
    /// Before the first send of a transaction, including authenticated retries.
    OutgoingRequest,
    /// Local hangup/cancellation intent, before waiting for an ACK or peer.
    LocalTerminationRequested,
    /// A BYE passed dialog validation. Its request and occurrence-time facts
    /// can be committed with application state before the successful reply.
    RemoteTerminationReceived,
    /// Before sending a final response to an incoming request.
    BeforeResponse,
    /// An INVITE final response was received; before its automatic ACK.
    /// Includes failures/challenges, re-INVITEs, and fork cleanup. A successful
    /// re-INVITE's snapshot already contains its accepted Contact. The message
    /// preserves the observed response; lifecycle facts identify initial acceptance.
    BeforeAck,
    /// A completed ACK/negotiation permits future quiescent restoration.
    SignalingCompleted,
    /// The dialog ended, including local rejection and protocol timeout.
    Terminated,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DialogCheckpoint {
    pub reason: DialogCheckpointReason,
    pub snapshot: DialogSnapshot,
    /// Intended or observed SIP message, when the checkpoint protects a send.
    /// This lets the application commit its SDP or request metadata atomically.
    pub message: Option<SipMessage>,
}

#[async_trait]
pub trait DialogPersistence: Send + Sync {
    async fn save(&self, checkpoint: DialogCheckpoint) -> crate::Result<()>;
}

#[derive(Clone)]
pub struct DialogPersistenceConfig {
    pub handler: Arc<dyn DialogPersistence>,
    pub timeout: Duration,
}

impl DialogPersistenceConfig {
    pub fn new(handler: Arc<dyn DialogPersistence>) -> Self {
        Self {
            handler,
            timeout: Duration::from_secs(5),
        }
    }

    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.timeout.is_zero() {
            return Err(crate::Error::Error(
                "persistence timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// A persistence failure with a machine-readable commit-outcome distinction.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PersistenceFailure {
    #[error("dialog persistence failed: {0}")]
    Storage(String),
    #[error("dialog persistence timed out; commit outcome is unknown")]
    TimedOut,
    #[error("dialog persistence worker stopped; commit outcome may be unknown")]
    WorkerStopped,
    #[error("checkpoint revision moved backwards")]
    RevisionMovedBackwards,
}
impl PersistenceFailure {
    /// Storage errors are application-reported failures; timeout/worker loss
    /// cannot prove whether an already started write committed.
    pub fn commit_outcome_unknown(&self) -> bool {
        matches!(self, Self::TimedOut | Self::WorkerStopped)
    }
}

/// A FIFO cutoff of already accepted writes, independent of snapshot revision.
/// Creating it is synchronous; waiting or dropping the waiter does not admit or
/// cancel any writes. It does not stop incoming signaling or drain transactions.
#[must_use]
pub struct PersistenceBarrier {
    receiver: Option<mpsc::UnboundedReceiver<core::result::Result<(), PersistenceFailure>>>,
}
impl PersistenceBarrier {
    #[cfg_attr(not(feature = "platform-tokio"), allow(unused_mut))]
    pub async fn wait(mut self) -> core::result::Result<(), PersistenceFailure> {
        match self.receiver.as_mut() {
            None => Ok(()),
            Some(receiver) => receiver
                .recv()
                .await
                .unwrap_or(Err(PersistenceFailure::WorkerStopped)),
        }
    }
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DialogPersistenceStatus {
    pub last_committed_revision: Option<u64>,
    pub failure: Option<String>,
    pub failure_kind: Option<PersistenceFailure>,
}

type CommitResult = core::result::Result<(), String>;
pub(crate) type CommitReceiver = mpsc::UnboundedReceiver<CommitResult>;

struct Work {
    checkpoint: DialogCheckpoint,
    reply: mpsc::UnboundedSender<CommitResult>,
}

enum WorkItem {
    Save(Work),
    Barrier(mpsc::UnboundedSender<core::result::Result<(), PersistenceFailure>>),
    Stop,
}

pub(crate) struct PersistenceRuntime {
    sender: mpsc::UnboundedSender<WorkItem>,
    pub status: Arc<Mutex<DialogPersistenceStatus>>,
}

impl PersistenceRuntime {
    #[cfg_attr(not(feature = "platform-tokio"), allow(unused_mut))]
    pub fn start(dialog: Weak<DialogInner>, config: DialogPersistenceConfig) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<WorkItem>();
        let status = Arc::new(Mutex::new(DialogPersistenceStatus::default()));
        let worker_status = status.clone();
        // The worker never owns a strong dialog reference while awaiting storage.
        // Dropping the dialog queues a stop marker; pending writes have finite timeouts.
        crate::platform::spawn(async move {
            while let Some(item) = receiver.recv().await {
                let work = match item {
                    WorkItem::Stop => break,
                    WorkItem::Barrier(reply) => {
                        let result = worker_status
                            .lock()
                            .failure_kind
                            .clone()
                            .map_or(Ok(()), Err);
                        let _ = reply.send(result);
                        continue;
                    }
                    WorkItem::Save(work) => work,
                };
                let revision = work.checkpoint.snapshot.runtime.revision;
                let sequence = work
                    .checkpoint
                    .snapshot
                    .runtime
                    .lifecycle
                    .last()
                    .map_or(0, |fact| fact.sequence);
                let previous = worker_status.lock().clone();
                let result = if let Some(error) = previous.failure_kind {
                    Err(error)
                } else if previous
                    .last_committed_revision
                    .is_some_and(|r| r > revision)
                {
                    Err(PersistenceFailure::RevisionMovedBackwards)
                } else if work.checkpoint.reason == DialogCheckpointReason::Terminated
                    && previous.last_committed_revision == Some(revision)
                {
                    Ok(())
                } else {
                    match crate::platform::timeout(
                        config.timeout,
                        config.handler.save(work.checkpoint),
                    )
                    .await
                    {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(PersistenceFailure::Storage(error.to_string())),
                        Err(_) => Err(PersistenceFailure::TimedOut),
                    }
                };
                match &result {
                    Ok(()) => {
                        worker_status.lock().last_committed_revision = Some(revision);
                        if let Some(dialog) = dialog.upgrade() {
                            dialog.release_committed_facts(sequence);
                        }
                    }
                    Err(error) => {
                        {
                            let mut status = worker_status.lock();
                            status.failure = Some(error.to_string());
                            status.failure_kind = Some(error.clone());
                        }
                        tracing::warn!(%error, revision, "dialog persistence stopped");
                    }
                }
                let _ = work.reply.send(result.map_err(|error| error.to_string()));
            }
        });
        Arc::new(Self { sender, status })
    }

    fn barrier(&self) -> PersistenceBarrier {
        let (reply, receiver) = mpsc::unbounded_channel();
        if let Err(error) = self.sender.send(WorkItem::Barrier(reply)) {
            if let WorkItem::Barrier(reply) = error.0 {
                let _ = reply.send(Err(PersistenceFailure::WorkerStopped));
            }
        }
        PersistenceBarrier {
            receiver: Some(receiver),
        }
    }

    /// Enqueue while the snapshot gate is held; user code runs in the worker.
    pub fn submit(&self, checkpoint: DialogCheckpoint) -> CommitReceiver {
        let (reply, receiver) = mpsc::unbounded_channel();
        if let Some(error) = self.status.lock().failure.clone() {
            let _ = reply.send(Err(error));
            return receiver;
        }
        if let Err(error) = self.sender.send(WorkItem::Save(Work { checkpoint, reply })) {
            if let WorkItem::Save(work) = error.0 {
                let _ = work
                    .reply
                    .send(Err("dialog persistence worker stopped".into()));
            }
        }
        receiver
    }
}

impl DialogInner {
    pub(crate) fn persistence_barrier(&self) -> PersistenceBarrier {
        // The same gate orders checkpoint submission and this cutoff.
        let _state = self.state.lock();
        self.persistence
            .lock()
            .as_ref()
            .map_or(PersistenceBarrier { receiver: None }, |runtime| {
                runtime.barrier()
            })
    }

    pub(crate) fn configure_persistence(self: &Arc<Self>, config: Option<DialogPersistenceConfig>) {
        if let Some(config) = config {
            *self.persistence_owner.lock() = Arc::downgrade(self);
            self.lifecycle.lock().defer_delivery = true;
            *self.persistence.lock() =
                Some(PersistenceRuntime::start(Arc::downgrade(self), config));
        }
    }

    pub(crate) fn bind_persistence(&self, tx: &crate::transaction::transaction::Transaction) {
        *tx.persistence_dialog.lock() = self.persistence_owner.lock().clone();
    }

    pub(crate) fn release_committed_facts(&self, sequence: u64) {
        let _state = self.state.lock();
        let mut lifecycle = self.lifecycle.lock();
        let mut waiting: Vec<DialogLifecycleEvent> = Vec::new();
        for event in core::mem::take(&mut lifecycle.undelivered) {
            if event.sequence <= sequence {
                if let Some(sender) = &lifecycle.sender {
                    let _ = sender.send(event);
                }
            } else {
                waiting.push(event);
            }
        }
        lifecycle.undelivered = waiting;
    }

    #[cfg_attr(not(feature = "platform-tokio"), allow(unused_mut))]
    pub(crate) async fn checkpoint(
        &self,
        reason: DialogCheckpointReason,
        message: Option<SipMessage>,
    ) -> crate::Result<()> {
        let receiver = {
            let state = self.state.lock();
            self.enqueue_checkpoint_locked(&state, reason, message)
        };
        if let Some(mut receiver) = receiver {
            receiver
                .recv()
                .await
                .unwrap_or_else(|| Err("dialog persistence worker stopped".into()))
                .map_err(crate::Error::Error)?;
        }
        Ok(())
    }

    pub(crate) fn enqueue_checkpoint_locked(
        &self,
        state: &super::dialog::DialogState,
        reason: DialogCheckpointReason,
        message: Option<SipMessage>,
    ) -> Option<CommitReceiver> {
        let runtime = self.persistence.lock().clone()?;
        Some(runtime.submit(DialogCheckpoint {
            reason,
            snapshot: self.snapshot_locked(state),
            message,
        }))
    }
}

impl Drop for PersistenceRuntime {
    fn drop(&mut self) {
        // Also works on runtimes whose channels do not close on last-sender drop.
        // FIFO drains already accepted writes before stopping the worker.
        let _ = self.sender.send(WorkItem::Stop);
    }
}

#[cfg(all(test, feature = "platform-tokio"))]
mod barrier_tests {
    use super::*;
    use crate::{
        dialog::{dialog::DialogSnapshotState, snapshot::DialogSnapshotRuntime, DialogId},
        sip::ToTypedHeader,
        transaction::key::TransactionRole,
    };
    use tokio::sync::{mpsc, oneshot};
    struct Storage(mpsc::UnboundedSender<oneshot::Sender<crate::Result<()>>>);
    #[async_trait]
    impl DialogPersistence for Storage {
        async fn save(&self, _: DialogCheckpoint) -> crate::Result<()> {
            let (reply, result) = oneshot::channel();
            self.0.send(reply).unwrap();
            result.await.unwrap()
        }
    }
    fn checkpoint() -> DialogCheckpoint {
        let snapshot = DialogSnapshot::builder(
            DialogId {
                call_id: "barrier".into(),
                local_tag: "local".into(),
                remote_tag: "peer".into(),
            },
            TransactionRole::Client,
            crate::sip::From::from("<sip:local@localhost>;tag=local")
                .typed()
                .unwrap(),
            crate::sip::To::from("<sip:peer@localhost>;tag=peer")
                .typed()
                .unwrap(),
            "sip:peer@localhost".try_into().unwrap(),
        )
        .state(DialogSnapshotState::Confirmed)
        .local_cseq(1)
        .remote_cseq(0)
        .runtime(DialogSnapshotRuntime::default())
        .build()
        .unwrap();
        DialogCheckpoint {
            snapshot,
            reason: DialogCheckpointReason::BeforeAck,
            message: None,
        }
    }
    fn worker(
        timeout: Duration,
    ) -> (
        Arc<PersistenceRuntime>,
        mpsc::UnboundedReceiver<oneshot::Sender<crate::Result<()>>>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut config = DialogPersistenceConfig::new(Arc::new(Storage(sender)));
        config.timeout = timeout;
        (PersistenceRuntime::start(Weak::new(), config), receiver)
    }
    #[tokio::test]
    async fn barrier_covers_same_revision_writes_but_not_later_work() {
        let (runtime, mut writes) = worker(Duration::from_secs(2));
        let _first = runtime.submit(checkpoint());
        let _second = runtime.submit(checkpoint());
        let barrier = runtime.barrier();
        let _third = runtime.submit(checkpoint());
        let waiting = tokio::spawn(barrier.wait());
        writes.recv().await.unwrap().send(Ok(())).unwrap();
        let second = writes.recv().await.unwrap();
        assert_eq!(runtime.status.lock().last_committed_revision, Some(0));
        assert!(
            !waiting.is_finished(),
            "revision 0 does not cover the second write"
        );
        second.send(Ok(())).unwrap();
        let third = writes.recv().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        third.send(Ok(())).unwrap();
        runtime.barrier().wait().await.unwrap();
    }
    #[tokio::test]
    async fn dropped_waiter_and_runtime_still_drain_accepted_writes() {
        let (runtime, mut writes) = worker(Duration::from_secs(2));
        let mut result = runtime.submit(checkpoint());
        drop(runtime.barrier());
        drop(runtime);
        writes.recv().await.unwrap().send(Ok(())).unwrap();
        assert!(result.recv().await.unwrap().is_ok());
        assert!(
            writes.recv().await.is_none(),
            "worker must release storage after drain"
        );
    }
    #[tokio::test]
    async fn barrier_exposes_storage_failure_and_poisoned_queue() {
        let (runtime, mut writes) = worker(Duration::from_secs(2));
        let _result = runtime.submit(checkpoint());
        let barrier = runtime.barrier();
        writes
            .recv()
            .await
            .unwrap()
            .send(Err(crate::Error::Error("disk failure".into())))
            .unwrap();
        assert!(matches!(
            barrier.wait().await,
            Err(PersistenceFailure::Storage(_))
        ));
        let mut rejected = runtime.submit(checkpoint());
        assert!(rejected.recv().await.unwrap().is_err());
        assert!(matches!(
            runtime.barrier().wait().await,
            Err(PersistenceFailure::Storage(_))
        ));
        assert!(writes.try_recv().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn barrier_timeout_reports_unknown_commit_outcome() {
        let (runtime, mut writes) = worker(Duration::from_secs(1));
        let _result = runtime.submit(checkpoint());
        let barrier = runtime.barrier();
        let _blocked = writes.recv().await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        let error = barrier.wait().await.unwrap_err();
        assert_eq!(error, PersistenceFailure::TimedOut);
        assert!(error.commit_outcome_unknown());
    }
}

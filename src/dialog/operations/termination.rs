//! Owned termination of an INVITE session, independent of its current phase.
//!
//! The first admitted intent wins. Repeated calls observe the same operation;
//! dropping observers does not cancel it. Early UAC calls are cancelled, early
//! UAS calls are rejected, and accepted sessions are ended with BYE after the
//! required ACK boundary. An already ended session needs no additional request.
use super::{
    dialog::DialogState,
    initial_operation::{CancelCompletion, CancelOperation, InitialControl},
    invite_dialog::InviteDialog,
    operation::{
        AdmissionError, DialogOperation, OperationCompletion, OperationError, OperationId,
    },
    persistence::DialogCheckpointReason,
    response_operation::{ResponseError, ResponseProgress},
};
use crate::{
    platform::{self, sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, Method, StatusCode},
    transaction::key::TransactionRole,
};
use core::sync::atomic::Ordering;

/// Application policy for ending a call. Identity, CSeq and routing are owned
/// by the stack. Reason headers also accompany a generated early CANCEL.
#[derive(Clone, Debug)]
pub struct TerminationOptions {
    headers: Option<Vec<Header>>,
    rejection: StatusCode,
}
impl Default for TerminationOptions {
    fn default() -> Self {
        Self {
            headers: None,
            rejection: StatusCode::Decline,
        }
    }
}
impl TerminationOptions {
    pub fn with_headers(mut self, headers: Vec<Header>) -> Self {
        self.headers = Some(headers);
        self
    }
    pub fn with_reason(self, reason: impl Into<String>) -> Self {
        let mut options = self;
        options
            .headers
            .get_or_insert_with(Vec::new)
            .push(Header::Reason(reason.into().into()));
        options
    }
    /// Final response for an unanswered incoming INVITE (300..699).
    pub fn with_rejection(mut self, status: StatusCode) -> Self {
        self.rejection = status;
        self
    }
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TerminationProgress {
    pub cancellation: Option<CancelCompletion>,
    pub initial_invite: Option<OperationCompletion>,
    pub rejection: Option<ResponseProgress>,
    pub bye: Option<OperationCompletion>,
}
#[derive(Clone, Debug)]
pub struct TerminationCompletion {
    progress: TerminationProgress,
    state: DialogState,
    result: core::result::Result<(), OperationError>,
}
impl TerminationCompletion {
    pub fn progress(&self) -> &TerminationProgress {
        &self.progress
    }
    pub fn dialog_state(&self) -> &DialogState {
        &self.state
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        self.result.as_ref().map(|_| ())
    }
}
struct Trace {
    id: OperationId,
    progress: Mutex<TerminationProgress>,
    completion: Mutex<Option<TerminationCompletion>>,
    done: CancellationToken,
}
impl Trace {
    fn complete(&self, dialog: &InviteDialog, result: core::result::Result<(), OperationError>) {
        let mut completion = self.completion.lock();
        if completion.is_none() {
            *completion = Some(TerminationCompletion {
                progress: self.progress.lock().clone(),
                state: dialog.state(),
                result,
            });
            self.done.cancel();
        }
    }
}
#[derive(Clone)]
#[must_use]
pub struct TerminationOperation {
    trace: Arc<Trace>,
}
impl TerminationOperation {
    pub fn id(&self) -> &OperationId {
        &self.trace.id
    }
    pub fn progress(&self) -> TerminationProgress {
        self.trace.progress.lock().clone()
    }
    /// Completion covers the chosen signaling and its required checkpoints,
    /// not retirement of every transaction or a process-wide drain.
    pub async fn wait(&self) -> TerminationCompletion {
        self.trace.done.cancelled().await;
        self.trace
            .completion
            .lock()
            .clone()
            .expect("termination result precedes notification")
    }
}
struct Driver {
    dialog: InviteDialog,
    trace: Arc<Trace>,
}
impl Drop for Driver {
    fn drop(&mut self) {
        // Interrupted work keeps pending recovery metadata, as other owned
        // operations do. A dropped observer never drops this driver.
        self.trace
            .complete(&self.dialog, Err(OperationError::DriverStopped));
    }
}

impl InviteDialog {
    /// End the session without selecting CANCEL, rejection or BYE in the
    /// application. The first accepted options apply to all later observers.
    /// Early outgoing calls must have been created through `start_invite`.
    ///
    /// ```no_run
    /// # use rsipstack::dialog::{invite_dialog::InviteDialog, termination::TerminationOptions};
    /// # async fn end_call(dialog: &InviteDialog) -> Result<(), Box<dyn std::error::Error>> {
    /// let operation = dialog.start_termination(
    ///     TerminationOptions::default().with_reason("SIP;cause=200;text=\"finished\""),
    /// )?;
    /// // The dialog may end before signaling completes. Keep the operation
    /// // to inspect the CANCEL, initial INVITE, rejection or BYE outcome.
    /// let completed = operation.wait().await;
    /// completed.signaling_result().map_err(Clone::clone)?;
    /// # Ok(())
    /// # }
    /// ```
    pub(crate) fn admit_termination(
        &self,
        options: TerminationOptions,
    ) -> core::result::Result<TerminationOperation, AdmissionError> {
        let mut admitted = self.inner.termination.lock();
        if let Some(operation) = &*admitted {
            return Ok(operation.clone());
        }
        if !(300..=699).contains(&options.rejection.code()) {
            return Err(AdmissionError::InvalidRequest(
                "termination rejection must be 300..699".into(),
            ));
        }
        if options.headers.iter().flatten().any(|h| {
            [
                "via",
                "v",
                "from",
                "f",
                "to",
                "t",
                "call-id",
                "i",
                "cseq",
                "route",
                "record-route",
            ]
            .iter()
            .any(|n| h.name().trim().eq_ignore_ascii_case(n))
        }) {
            return Err(AdmissionError::InvalidRequest(
                "identity, sequence, and routing headers are stack-owned".into(),
            ));
        }
        if let Some(failure) = self.persistence_status().and_then(|s| s.failure_kind) {
            return Err(AdmissionError::Persistence(failure));
        }
        let initial = self.inner.initial_operation.lock().upgrade();
        if self.role() == TransactionRole::Client && self.inner.can_cancel() && initial.is_none() {
            return Err(AdmissionError::InvalidRequest(
                "early termination requires an owned initial INVITE".into(),
            ));
        }
        let cancellation = initial
            .as_ref()
            .filter(|_| self.inner.can_cancel())
            .map(|control| {
                let reasons = options
                    .headers
                    .iter()
                    .flatten()
                    .filter(|h| matches!(h, Header::Reason(_)))
                    .cloned()
                    .collect();
                control.cancel_with_headers(reasons)
            });
        let trace = Arc::new(Trace {
            id: OperationId::admit(&self.inner),
            progress: Mutex::new(Default::default()),
            completion: Mutex::new(None),
            done: CancellationToken::new(),
        });
        let operation = TerminationOperation {
            trace: trace.clone(),
        };
        *admitted = Some(operation.clone());
        let dialog = self.clone();
        let guard = Driver {
            dialog: dialog.clone(),
            trace: trace.clone(),
        };
        platform::spawn(async move {
            let _guard = guard;
            let stop = dialog.inner.endpoint_inner.shutdown_token();
            let result = platform::select2(
                core::pin::pin!(dialog.drive_termination(options, initial, cancellation, &trace)),
                core::pin::pin!(stop.cancelled()),
            )
            .await;
            if let platform::Either::A(mut result) = result {
                dialog.inner.finish_owned_operation();
                if let Err(error) = dialog
                    .inner
                    .checkpoint(DialogCheckpointReason::SignalingCompleted, None)
                    .await
                {
                    result = Err(storage_or_protocol(&dialog, error));
                }
                trace.complete(&dialog, result);
            }
        });
        Ok(operation)
    }

    async fn drive_termination(
        &self,
        options: TerminationOptions,
        initial: Option<Arc<InitialControl>>,
        cancellation: Option<CancelOperation>,
        trace: &Trace,
    ) -> core::result::Result<(), OperationError> {
        if !self.state().is_terminated() {
            {
                let _state = self.inner.state.lock();
                self.inner.record_local_termination();
            }
            self.inner
                .checkpoint(DialogCheckpointReason::LocalTerminationRequested, None)
                .await
                .map_err(|e| storage_or_protocol(self, e))?;
        }
        let mut earlier_error = None;
        if let Some(cancellation) = cancellation {
            let result = cancellation.wait().await;
            earlier_error = result.signaling_result().err().cloned();
            trace.progress.lock().cancellation = Some(result);
        }
        if let Some(initial) = initial {
            let result = DialogOperation {
                trace: initial.trace.clone(),
            }
            .wait()
            .await;
            if let Err(error) = result.signaling_result() {
                if !matches!(error, OperationError::CancelledBeforeSend) {
                    earlier_error = Some(error.clone());
                }
            }
            trace.progress.lock().initial_invite = Some(result);
            trace.progress.lock().bye = initial.cleanup.lock().clone();
        }
        loop {
            // Observe a generation before state, avoiding lost wakeups.
            let changed = self.inner.state_changed.lock().clone();
            let state = self.state();
            if state.is_terminated() {
                return earlier_error.map_or(Ok(()), Err);
            }
            if let Some(error) = self.persistence_status().and_then(|s| s.failure_kind) {
                return Err(OperationError::Persistence(error));
            }
            let server_ack = self.inner.server_ack_state.load(Ordering::SeqCst);
            if state.is_confirmed() || (state.waiting_ack() && matches!(server_ack, 1 | 2)) {
                if server_ack == 2 {
                    self.inner.termination_timeout.store(true, Ordering::SeqCst);
                }
                let operation = match self.admit_operation(
                    Method::Bye,
                    options.headers.clone(),
                    None,
                    None,
                    false,
                ) {
                    Ok(operation) => operation,
                    Err(AdmissionError::NotConfirmed) if self.state().is_terminated() => continue,
                    Err(error) => return Err(OperationError::Protocol(error.to_string())),
                };
                let result = operation.wait().await;
                let error = result.signaling_result().err().cloned();
                trace.progress.lock().bye = Some(result);
                // A remote BYE can win between admission and the send turn.
                if matches!(error, Some(OperationError::DialogEnded))
                    && self.state().is_terminated()
                {
                    return earlier_error.map_or(Ok(()), Err);
                }
                return error.or(earlier_error).map_or(Ok(()), Err);
            }
            if self.role() == TransactionRole::Server {
                if server_ack == 3 {
                    return Err(OperationError::DriverStopped);
                }
                if self.inner.can_cancel() && !self.inner.initial_responses.final_admitted() {
                    match self.start_response(
                        options.rejection.clone(),
                        options.headers.clone(),
                        None,
                    ) {
                        Ok(receipt) => {
                            let sent = receipt.wait_sent().await;
                            trace.progress.lock().rejection = Some(receipt.progress());
                            return sent.map(|_| ()).map_err(response_error);
                        }
                        Err(ResponseError::FinalAlreadyAdmitted) => {}
                        Err(ResponseError::Protocol(_)) if !self.inner.can_cancel() => continue,
                        Err(error) => return Err(response_error(error)),
                    }
                }
            }
            changed.cancelled().await;
        }
    }
}
fn response_error(error: ResponseError) -> OperationError {
    match error {
        ResponseError::Persistence(error) => OperationError::Persistence(error),
        ResponseError::Transport(error) => OperationError::Transport(error),
        ResponseError::DriverStopped | ResponseError::Closed => OperationError::DriverStopped,
        error => OperationError::Protocol(error.to_string()),
    }
}
fn storage_or_protocol(dialog: &InviteDialog, error: crate::Error) -> OperationError {
    dialog
        .persistence_status()
        .and_then(|s| s.failure_kind)
        .map(OperationError::Persistence)
        .unwrap_or_else(|| OperationError::Protocol(error.to_string()))
}

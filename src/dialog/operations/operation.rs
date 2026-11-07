//! Owned in-dialog operations. Admission, SIP response, and session state are
//! separate. Dropping a handle or a wait future never cancels accepted work.
//! Commands have an operation identity independent of SIP CSeq. The driver
//! allocates CSeq only when its send slot is ready, then checkpoints the exact
//! request before transport submission. Responses can complete concurrently.
use alloc::sync::Weak;
use core::sync::atomic::{AtomicBool, Ordering};

use super::{
    dialog::{DialogInner, DialogState},
    invite_dialog::InviteDialog,
    persistence::PersistenceFailure,
    reinvite::ReinviteOptions,
    DialogId,
};
use crate::{
    platform::{sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, HeadersExt, Method, Request, Response, StatusCodeKind},
    transaction::key::TransactionRole,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationId {
    dialog: DialogId,
    sequence: u64,
}
impl OperationId {
    pub(super) fn admit(dialog: &Arc<DialogInner>) -> Self {
        let _state = dialog.state.lock();
        let mut lifecycle = dialog.lifecycle.lock();
        lifecycle.operations += 1;
        lifecycle.revision += 1;
        Self {
            dialog: dialog.id.lock().clone(),
            sequence: lifecycle.revision,
        }
    }
    pub fn dialog(&self) -> &DialogId {
        &self.dialog
    }
    /// Admission sequence within this live dialog instance, not a SIP CSeq.
    /// Storage idempotency also needs an application-owned incarnation key.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResponseOrigin {
    Peer,
    Local,
    /// Injected through the low-level TU channel without transport provenance.
    Unknown,
}
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdmissionError {
    #[error("owned operations require a dialog created or restored through DialogLayer")]
    ExternallyOwnedState,
    #[error("operation requires a confirmed dialog")]
    NotConfirmed,
    #[error("an SDP negotiation is already in progress")]
    NegotiationInProgress,
    #[error("{0}")]
    Persistence(PersistenceFailure),
    #[error("invalid operation: {0}")]
    InvalidRequest(String),
}
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OperationError {
    #[error("{0}")]
    Persistence(PersistenceFailure),
    #[error("ACK failed: {0}")]
    Ack(String),
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("transaction timed out")]
    Timeout,
    #[error("dialog ended before the operation could send")]
    DialogEnded,
    #[error("protocol operation failed: {0}")]
    Protocol(String),
    #[error("operation cancelled before its first send")]
    CancelledBeforeSend,
    #[error("operation driver stopped before completion")]
    DriverStopped,
}
#[derive(Clone, Debug)]
pub struct OperationAttempt {
    request: Request,
    checkpoint_committed: bool,
    transport_accepted: bool,
    response: Option<Response>,
    origin: Option<ResponseOrigin>,
    ack_sent: bool,
    error: Option<OperationError>,
}
impl OperationAttempt {
    pub fn request(&self) -> &Request {
        &self.request
    }
    /// False also covers persistence being disabled.
    pub fn checkpoint_committed(&self) -> bool {
        self.checkpoint_committed
    }
    /// At least one transport send succeeded; remote receipt is not implied.
    pub fn transport_accepted(&self) -> bool {
        self.transport_accepted
    }
    pub fn response(&self) -> Option<&Response> {
        self.response.as_ref()
    }
    pub fn response_origin(&self) -> Option<ResponseOrigin> {
        self.origin
    }
    pub fn ack_sent(&self) -> bool {
        self.ack_sent
    }
    pub fn error(&self) -> Option<&OperationError> {
        self.error.as_ref()
    }
}
#[derive(Clone, Debug)]
pub struct OperationCompletion {
    owner: Weak<DialogInner>,
    cleanup_started: Arc<AtomicBool>,
    id: OperationId,
    attempts: Vec<OperationAttempt>,
    state: DialogState,
    result: core::result::Result<(), OperationError>,
}
impl OperationCompletion {
    pub fn id(&self) -> &OperationId {
        &self.id
    }
    pub fn attempts(&self) -> &[OperationAttempt] {
        &self.attempts
    }
    /// Final response of the last attempt, including a marked local timeout.
    /// Earlier authentication challenges remain available through `attempts()`.
    pub fn response(&self) -> Option<&Response> {
        self.attempts.last().and_then(|a| a.response())
    }
    pub fn response_origin(&self) -> Option<ResponseOrigin> {
        self.attempts.last().and_then(|a| a.origin)
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        self.result.as_ref().map(|_| ())
    }
    pub fn dialog_state(&self) -> &DialogState {
        &self.state
    }
}
struct TraceState {
    attempts: Vec<OperationAttempt>,
    completion: Option<OperationCompletion>,
    admission_failure: Option<OperationError>,
}
pub(crate) struct OperationTrace {
    id: OperationId,
    state: Mutex<TraceState>,
    done: CancellationToken,
    first_send: CancellationToken,
    pub(crate) provisional: Mutex<Option<crate::platform::mpsc::UnboundedSender<Response>>>,
    cancellation: Option<CancellationToken>,
}
impl OperationTrace {
    pub(crate) fn admitted(dialog: &Arc<DialogInner>) -> Arc<Self> {
        Self::new(OperationId::admit(dialog), None)
    }
    pub(crate) fn new(id: OperationId, cancellation: Option<CancellationToken>) -> Arc<Self> {
        Arc::new(Self {
            id,
            state: Mutex::new(TraceState {
                attempts: Vec::new(),
                completion: None,
                admission_failure: None,
            }),
            done: CancellationToken::new(),
            first_send: CancellationToken::new(),
            provisional: Mutex::new(None),
            cancellation,
        })
    }
    pub(crate) fn attempt(&self, request: &Request) {
        let mut state = self.state.lock();
        if state.completion.is_some() {
            return;
        }
        if state
            .attempts
            .last()
            .is_some_and(|a| a.request.cseq_header().ok() == request.cseq_header().ok())
        {
            return;
        }
        state.attempts.push(OperationAttempt {
            request: request.clone(),
            checkpoint_committed: false,
            transport_accepted: false,
            response: None,
            origin: None,
            ack_sent: false,
            error: None,
        });
    }
    fn update(&self, f: impl FnOnce(&mut OperationAttempt)) {
        let mut state = self.state.lock();
        if state.completion.is_none() {
            if let Some(attempt) = state.attempts.last_mut() {
                f(attempt);
            }
        }
    }
    pub(crate) fn saved(&self) {
        self.update(|a| a.checkpoint_committed = true);
    }
    pub(crate) fn sent(&self) {
        self.update(|a| a.transport_accepted = true);
        self.first_send.cancel();
    }
    pub(crate) fn acked(&self) {
        self.update(|a| a.ack_sent = true);
    }
    pub(crate) fn failed(&self, error: OperationError) {
        let mut state = self.state.lock();
        if state.completion.is_none() {
            if let Some(attempt) = state.attempts.last_mut() {
                attempt.error = Some(error);
            } else {
                state.admission_failure = Some(error);
            }
        }
    }
    pub(crate) fn response(&self, response: &Response, origin: ResponseOrigin) {
        if response.status_code.kind() == StatusCodeKind::Provisional {
            if let Some(sender) = self.provisional.lock().as_ref() {
                sender.send(response.clone()).ok();
            }
            return;
        }
        if origin != ResponseOrigin::Local
            && !DialogId::try_from((response, TransactionRole::Client)).is_ok_and(|id| {
                id == self.id.dialog
                    || (self.id.dialog.remote_tag.is_empty()
                        && id.call_id == self.id.dialog.call_id
                        && id.local_tag == self.id.dialog.local_tag)
            })
        {
            return;
        }
        self.update(|a| {
            if origin == ResponseOrigin::Peer
                && matches!(a.error, Some(OperationError::Transport(_)))
            {
                a.error = None;
            }
            a.response = Some(response.clone());
            a.origin = Some(origin);
        });
    }
    pub(crate) fn complete(
        &self,
        dialog: &Arc<DialogInner>,
        result: crate::Result<Option<Response>>,
    ) {
        let dialog_state = dialog.state.lock().clone();
        let failure = dialog
            .persistence
            .lock()
            .as_ref()
            .and_then(|r| r.status.lock().failure_kind.clone());
        let mut state = self.state.lock();
        if state.completion.is_some() {
            return;
        }
        let result = if let Some(failure) = failure {
            Err(OperationError::Persistence(failure))
        } else if let Some(error) = state
            .admission_failure
            .clone()
            .or_else(|| state.attempts.last().and_then(|a| a.error.clone()))
        {
            Err(error)
        } else {
            match result {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err(OperationError::Timeout),
                Err(error) => Err(OperationError::Protocol(error.to_string())),
            }
        };
        state.completion = Some(OperationCompletion {
            owner: Arc::downgrade(dialog),
            cleanup_started: Arc::new(AtomicBool::new(false)),
            id: self.id.clone(),
            attempts: state.attempts.clone(),
            state: dialog_state,
            result,
        });
        drop(state);
        self.done.cancel();
        self.first_send.cancel();
    }
}
/// Cloneable observer; all waiters see the same completion snapshot.
#[derive(Clone)]
#[must_use]
pub struct DialogOperation {
    pub(crate) trace: Arc<OperationTrace>,
}
impl DialogOperation {
    /// Observe the first transport-accepted request without waiting for its
    /// final response. A terminal failure before any send is returned as an
    /// error; dropping this observer never cancels the admitted operation.
    pub async fn wait_first_send(&self) -> core::result::Result<OperationAttempt, OperationError> {
        self.trace.first_send.cancelled().await;
        let state = self.trace.state.lock();
        if let Some(attempt) = state.attempts.iter().find(|a| a.transport_accepted) {
            return Ok(attempt.clone());
        }
        Err(state
            .completion
            .as_ref()
            .and_then(|c| c.result.as_ref().err())
            .cloned()
            .or_else(|| state.admission_failure.clone())
            .unwrap_or(OperationError::DriverStopped))
    }
    pub fn id(&self) -> &OperationId {
        &self.trace.id
    }
    pub fn attempts(&self) -> Vec<OperationAttempt> {
        self.trace.state.lock().attempts.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.trace.done.is_cancelled()
    }
    /// Request SIP CANCEL for a re-INVITE. The operation still processes its
    /// final response and ACK. Other methods have no SIP CANCEL semantics here.
    pub fn request_cancellation(&self) -> bool {
        if let Some(token) = &self.trace.cancellation {
            token.cancel();
            true
        } else {
            false
        }
    }
    pub async fn wait(&self) -> OperationCompletion {
        self.trace.done.cancelled().await;
        self.trace
            .state
            .lock()
            .completion
            .clone()
            .expect("completion precedes notification")
    }
}
/// A first-send ordering slot. Responses from different operations can overlap.
pub(crate) struct SendOrder {
    pub(crate) previous: Option<CancellationToken>,
    done: CancellationToken,
}
impl SendOrder {
    pub(crate) async fn wait(&self) {
        if let Some(previous) = &self.previous {
            previous.cancelled().await;
        }
    }
}
impl Drop for SendOrder {
    fn drop(&mut self) {
        self.done.cancel();
    }
}

impl InviteDialog {
    /// Admit an owned command. CSeq and the final request are allocated by the
    /// driver, then exposed to the optional persistence checkpoint before send.
    pub(crate) fn admit_request(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        if matches!(
            method,
            Method::Invite | Method::Cancel | Method::Ack | Method::Register
        ) {
            return Err(AdmissionError::InvalidRequest(
                "use start_reinvite for INVITE; ACK/CANCEL are stack-owned".into(),
            ));
        }
        self.admit_operation(method, headers, body, None, false)
    }
    pub(crate) fn admit_reinvite(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        options: ReinviteOptions,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        if body.as_ref().is_none_or(|b| b.is_empty()) != options.answer_handler.is_some() {
            return Err(AdmissionError::InvalidRequest(
                "offerless INVITE requires an ACK answer handler".into(),
            ));
        }
        self.admit_operation(Method::Invite, headers, body, Some(options), false)
    }
    /// Explicit application policy for an already-ended session whose pending
    /// re-INVITE subsequently succeeded. Sends a fresh BYE using the accepted
    /// target, after the matching ACK; never revives the session. A completion
    /// can authorize this cleanup once and only on its original live instance.
    /// Normal BYE does not automatically adopt this application cleanup policy.
    pub(crate) fn admit_late_answer_cleanup(
        &self,
        completion: &OperationCompletion,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let valid = completion.owner.ptr_eq(&Arc::downgrade(&self.inner))
            && completion.dialog_state().is_terminated()
            && self.state().is_terminated()
            && completion.signaling_result().is_ok()
            && completion.attempts().last().is_some_and(|a| {
                a.request().method == Method::Invite
                    && a.ack_sent()
                    && a.response_origin() == Some(ResponseOrigin::Peer)
                    && a.response()
                        .is_some_and(|r| r.status_code.kind() == StatusCodeKind::Successful)
            });
        if !valid {
            return Err(AdmissionError::InvalidRequest(
                "cleanup requires this instance's ACKed late successful re-INVITE".into(),
            ));
        }
        if completion.cleanup_started.swap(true, Ordering::SeqCst) {
            return Err(AdmissionError::InvalidRequest(
                "late-answer cleanup already admitted".into(),
            ));
        }
        let result = self.admit_operation(Method::Bye, headers, None, None, true);
        if result.is_err() {
            completion.cleanup_started.store(false, Ordering::SeqCst);
        }
        result
    }

    pub(super) fn admit_operation(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        options: Option<ReinviteOptions>,
        late_cleanup: bool,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        if headers.as_ref().is_some_and(|headers| {
            headers.iter().any(|h| {
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
                .any(|name| h.name().trim().eq_ignore_ascii_case(name))
            })
        }) {
            return Err(AdmissionError::InvalidRequest(
                "identity, sequence, and routing headers are stack-owned".into(),
            ));
        }
        let (reservation, id, order) = {
            let state = self.inner.state.lock();
            let acknowledged_server = method == Method::Bye
                && self.role() == TransactionRole::Server
                && state.waiting_ack()
                && matches!(self.inner.server_ack_state.load(Ordering::SeqCst), 1 | 2);
            if !(state.is_confirmed()
                || acknowledged_server
                || (late_cleanup && state.is_terminated()))
            {
                return Err(AdmissionError::NotConfirmed);
            }
            if let Some(failure) = self.persistence_status().and_then(|s| s.failure_kind) {
                return Err(AdmissionError::Persistence(failure));
            }
            let reservation = if method == Method::Invite {
                Some(
                    crate::transaction::invite_usage::InviteReservation::acquire(
                        &self.inner.invite_usage,
                        TransactionRole::Client,
                    )
                    .map_err(|_| AdmissionError::NegotiationInProgress)?,
                )
            } else {
                None
            };
            let mut lifecycle = self.inner.lifecycle.lock();
            lifecycle.operations += 1;
            lifecycle.revision += 1;
            let id = OperationId {
                dialog: self.id(),
                sequence: lifecycle.revision,
            };
            (reservation, id, self.inner.reserve_send_order())
        };
        let trace = OperationTrace::new(id, options.as_ref().map(|o| o.cancellation.clone()));
        let operation = DialogOperation {
            trace: trace.clone(),
        };
        let inner = self.inner.clone();
        crate::platform::spawn(async move {
            let _guard = OperationDriver {
                dialog: inner.clone(),
                trace: trace.clone(),
            };
            let mut result = async {
                order.wait().await;
                if options
                    .as_ref()
                    .is_some_and(|o| o.cancellation.is_cancelled())
                {
                    trace.failed(OperationError::CancelledBeforeSend);
                    return Err(crate::Error::Error("cancelled before send".into()));
                }
                if inner.is_terminated() && !late_cleanup {
                    trace.failed(OperationError::DialogEnded);
                    return Err(crate::Error::Error("dialog ended before send".into()));
                }
                let request = inner.make_request(method, None, None, None, headers, body)?;
                let mut tx = inner.create_dialog_transaction_reserved(
                    request,
                    options.as_ref(),
                    reservation,
                )?;
                trace.attempt(&tx.original);
                tx.operation = Some(trace.clone());
                inner.send_owned_transaction(tx, options, Some(order)).await
            }
            .await;
            inner.finish_owned_operation();
            if let Err(error) = inner
                .checkpoint(
                    super::persistence::DialogCheckpointReason::SignalingCompleted,
                    None,
                )
                .await
            {
                result = Err(error);
            }
            trace.complete(&inner, result);
        });
        Ok(operation)
    }
}

struct OperationDriver {
    dialog: Arc<DialogInner>,
    trace: Arc<OperationTrace>,
}
impl Drop for OperationDriver {
    fn drop(&mut self) {
        if !self.trace.done.is_cancelled() {
            // An interrupted driver leaves pending metadata intact for recovery.
            self.trace.failed(OperationError::DriverStopped);
            self.trace.complete(
                &self.dialog,
                Err(crate::Error::Error("operation driver stopped".into())),
            );
        }
    }
}
impl DialogInner {
    pub(crate) fn reserve_send_order(&self) -> SendOrder {
        let done = CancellationToken::new();
        let previous = self.operation_tail.lock().replace(done.clone());
        SendOrder { previous, done }
    }
    pub(crate) fn finish_owned_operation(&self) {
        let _state = self.state.lock();
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.operations -= 1;
        lifecycle.revision += 1;
    }
}

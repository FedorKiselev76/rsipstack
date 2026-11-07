//! Receipts for admitted UAS responses. Queue admission, durable preparation,
//! transport acceptance and the matching INVITE ACK are separate observations.
use super::persistence::PersistenceFailure;
use crate::{
    platform::{sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, Method, Request, Response, StatusCode, StatusCodeKind},
};

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResponseError {
    #[error("response command queue is full")]
    Overloaded,
    #[error("response command receiver is closed")]
    Closed,
    #[error("response owner stopped before completing the requested stage")]
    DriverStopped,
    #[error("{0}")]
    Persistence(PersistenceFailure),
    #[error("response transport failed: {0}")]
    Transport(String),
    #[error("response rejected: {0}")]
    Protocol(String),
    #[error("this response has no INVITE ACK")]
    AckNotApplicable,
    #[error("INVITE ended without its matching ACK")]
    AckTimeout,
    #[error("a final response has already been admitted")]
    FinalAlreadyAdmitted,
    #[error("queued response was superseded by transaction completion")]
    Superseded,
}

pub(super) struct ResponseAdmission(Mutex<bool>);
impl Default for ResponseAdmission {
    fn default() -> Self {
        Self(Mutex::new(false))
    }
}
impl ResponseAdmission {
    pub(super) fn final_admitted(&self) -> bool {
        *self.0.lock()
    }
    pub(super) fn submit<T>(
        &self,
        status: &StatusCode,
        send: impl FnOnce() -> core::result::Result<T, ResponseError>,
    ) -> core::result::Result<T, ResponseError> {
        let mut final_admitted = self.0.lock();
        if *final_admitted {
            return Err(ResponseError::FinalAlreadyAdmitted);
        }
        let value = send()?;
        if status.kind() != StatusCodeKind::Provisional {
            *final_admitted = true;
        }
        Ok(value)
    }

    /// Conditionally reserve the single final response under the same lock as
    /// application submissions. A retired logical branch may decline to send.
    pub(super) fn submit_final_if<T>(
        &self,
        send: impl FnOnce() -> core::result::Result<Option<T>, ResponseError>,
    ) -> core::result::Result<Option<T>, ResponseError> {
        let mut final_admitted = self.0.lock();
        if *final_admitted {
            return Ok(None);
        }
        let value = send()?;
        if value.is_some() {
            *final_admitted = true;
        }
        Ok(value)
    }
}
#[derive(Clone, Debug, Default)]
pub struct ResponseProgress {
    pub response: Option<Response>,
    pub checkpoint_committed: bool,
    pub transport_accepted: bool,
    pub completion_committed: bool,
}
struct State {
    progress: ResponseProgress,
    sent: Option<core::result::Result<ResponseProgress, ResponseError>>,
    ack: Option<core::result::Result<Request, ResponseError>>,
}
pub(crate) struct ResponseTrace {
    state: Mutex<State>,
    sent: CancellationToken,
    ack: CancellationToken,
}
impl ResponseTrace {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                progress: ResponseProgress::default(),
                sent: None,
                ack: None,
            }),
            sent: CancellationToken::new(),
            ack: CancellationToken::new(),
        })
    }
    pub(crate) fn bind(&self, request: &Request, response: &Response) {
        self.state.lock().progress.response = Some(response.clone());
        if request.method != Method::Invite
            || response.status_code.kind() == StatusCodeKind::Provisional
        {
            self.acknowledge(Err(ResponseError::AckNotApplicable));
        }
    }
    pub(crate) fn prepared(&self, response: Response, saved: bool) {
        let mut state = self.state.lock();
        state.progress.response = Some(response);
        state.progress.checkpoint_committed = saved;
    }
    pub(crate) fn transported(&self) {
        self.state.lock().progress.transport_accepted = true;
    }
    pub(crate) fn transport_accepted(&self) -> bool {
        self.state.lock().progress.transport_accepted
    }
    pub(crate) fn complete(&self, result: core::result::Result<bool, ResponseError>) {
        let mut state = self.state.lock();
        if state.sent.is_some() {
            return;
        }
        state.sent = Some(match result {
            Ok(saved) => {
                state.progress.completion_committed = saved;
                Ok(state.progress.clone())
            }
            Err(error) => Err(error),
        });
        self.sent.cancel();
    }
    pub(crate) fn acknowledge(&self, result: core::result::Result<Request, ResponseError>) {
        let mut state = self.state.lock();
        if state.ack.is_none() {
            state.ack = Some(result);
            self.ack.cancel();
        }
    }
    fn stopped(&self) {
        self.complete(Err(ResponseError::DriverStopped));
        self.acknowledge(Err(ResponseError::DriverStopped));
    }
}
struct ProducerInner(Arc<ResponseTrace>);
impl Drop for ProducerInner {
    fn drop(&mut self) {
        self.0.stopped();
    }
}
#[derive(Clone)]
pub(crate) struct ResponseProducer(Arc<ProducerInner>);
impl ResponseProducer {
    pub(crate) fn trace(&self) -> &Arc<ResponseTrace> {
        &self.0 .0
    }
}
impl core::fmt::Debug for ResponseProducer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ResponseProducer").finish_non_exhaustive()
    }
}
#[derive(Clone)]
#[must_use]
pub struct ResponseReceipt {
    trace: Arc<ResponseTrace>,
}
impl ResponseReceipt {
    /// Inspect recorded progress, including transport acceptance when a later
    /// completion checkpoint failed. False flags can also mean disabled storage.
    pub fn progress(&self) -> ResponseProgress {
        self.trace.state.lock().progress.clone()
    }
    /// Wait for transmission and its required completion checkpoint. A queued
    /// 100rel response is not complete until PRACK permits its transmission.
    /// An error does not prove that nothing was sent; inspect `progress()`.
    pub async fn wait_sent(&self) -> core::result::Result<ResponseProgress, ResponseError> {
        self.trace.sent.cancelled().await;
        self.trace
            .state
            .lock()
            .sent
            .clone()
            .expect("send result precedes notification")
    }
    /// Wait for the matching final INVITE ACK. Provisional and non-INVITE
    /// responses have no such ACK. Dropping this wait never cancels the response.
    pub async fn wait_ack(&self) -> core::result::Result<Request, ResponseError> {
        self.trace.ack.cancelled().await;
        self.trace
            .state
            .lock()
            .ack
            .clone()
            .expect("ACK result precedes notification")
    }
    pub(crate) fn pair() -> (Self, ResponseProducer) {
        let trace = ResponseTrace::new();
        (
            Self {
                trace: trace.clone(),
            },
            ResponseProducer(Arc::new(ProducerInner(trace))),
        )
    }
}

/// Opaque tracked command carried by the low-level transaction event API.
#[derive(Clone, Debug)]
pub struct ResponseSubmission {
    pub(crate) response: Response,
    pub(crate) producer: Option<ResponseProducer>,
    pub(crate) initial_owner: Option<alloc::sync::Weak<super::dialog::DialogInner>>,
}
impl core::ops::Deref for ResponseSubmission {
    type Target = Response;
    fn deref(&self) -> &Response {
        &self.response
    }
}
impl core::ops::DerefMut for ResponseSubmission {
    fn deref_mut(&mut self) -> &mut Response {
        &mut self.response
    }
}
impl ResponseSubmission {
    pub(crate) fn untracked(response: Response) -> Self {
        Self {
            response,
            producer: None,
            initial_owner: None,
        }
    }
    pub(crate) fn event(self) -> crate::transaction::transaction::TransactionEvent {
        crate::transaction::transaction::TransactionEvent::RespondTracked(self)
    }
    pub(crate) fn supersede(self) {
        if let Some(producer) = self.producer {
            producer.trace().complete(Err(ResponseError::Superseded));
            producer.trace().acknowledge(Err(ResponseError::Superseded));
        }
    }
}
/// Opaque tracked command carried by a dialog's response channel.
#[derive(Debug)]
pub struct ResponseCommand {
    pub(crate) status: StatusCode,
    pub(crate) headers: Option<Vec<Header>>,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) producer: ResponseProducer,
}

impl super::dialog::DialogInner {
    pub(crate) fn initial_response_sent(&self, response: &Response) {
        use super::dialog::{DialogState, TerminatedReason};
        let id = self.id.lock().clone();
        match response.status_code.kind() {
            StatusCodeKind::Provisional if response.status_code != StatusCode::Trying => {
                self.transition(DialogState::Early(id, response.clone()))
            }
            StatusCodeKind::Successful => {
                self.transition(DialogState::WaitAck(id, response.clone()))
            }
            StatusCodeKind::Provisional => {}
            _ => self.transition(DialogState::Terminated(
                id,
                TerminatedReason::UasOther(response.status_code.clone()),
            )),
        }
    }
}

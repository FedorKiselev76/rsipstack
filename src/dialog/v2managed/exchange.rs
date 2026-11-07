//! Owned non-INVITE exchanges for event usages. No call-state compatibility API.
use super::*;
use crate::{
    dialog::authenticate::{handle_client_authenticate, AuthRetry},
    platform::{sync::Mutex, CancellationToken},
    sip::{HeadersExt, Response, SipMessage, StatusCodeKind},
};

#[derive(Clone, Debug)]
pub struct ExchangeAttempt {
    pub request: Request,
    pub transport_accepted: bool,
    pub response: Option<Response>,
    pub response_origin: Option<ResponseOrigin>,
}
#[derive(Clone, Debug)]
pub struct ExchangeCompletion {
    attempts: Vec<ExchangeAttempt>,
    result: core::result::Result<(), OperationError>,
}
impl ExchangeCompletion {
    pub fn attempts(&self) -> &[ExchangeAttempt] {
        &self.attempts
    }
    pub fn response(&self) -> Option<&Response> {
        self.attempts.iter().rev().find_map(|a| a.response.as_ref())
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        self.result.as_ref().map(|_| ())
    }
}
struct State {
    attempts: Vec<ExchangeAttempt>,
    result: Option<core::result::Result<(), OperationError>>,
}
pub(super) struct Trace {
    state: Mutex<State>,
    done: CancellationToken,
}
impl Trace {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                attempts: Vec::new(),
                result: None,
            }),
            done: CancellationToken::new(),
        })
    }
    pub fn finish(&self, result: core::result::Result<(), OperationError>) {
        let mut state = self.state.lock();
        if state.result.is_none() {
            state.result = Some(result);
            self.done.cancel();
        }
    }
}
#[derive(Clone)]
#[must_use]
pub struct ExchangeOperation {
    pub(super) trace: Arc<Trace>,
    pub(super) tracking_id: u64,
}
impl ExchangeOperation {
    /// Layer-local ID used by pending-operation diagnostics, not SIP CSeq.
    pub fn tracking_id(&self) -> u64 {
        self.tracking_id
    }
    pub fn is_finished(&self) -> bool {
        self.trace.done.is_cancelled()
    }
    pub fn attempts(&self) -> Vec<ExchangeAttempt> {
        self.trace.state.lock().attempts.clone()
    }
    pub async fn wait(&self) -> ExchangeCompletion {
        self.trace.done.cancelled().await;
        let state = self.trace.state.lock();
        ExchangeCompletion {
            attempts: state.attempts.clone(),
            result: state
                .result
                .clone()
                .unwrap_or(Err(OperationError::DriverStopped)),
        }
    }
}
pub(super) struct DriverGuard(pub Arc<Trace>);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.finish(Err(OperationError::DriverStopped));
    }
}

pub(super) struct Queue(Mutex<Option<CancellationToken>>);
impl Queue {
    pub fn new() -> Self {
        Self(Mutex::new(None))
    }
    pub fn reserve(&self) -> Ticket {
        let done = CancellationToken::new();
        let previous = self.0.lock().replace(done.clone());
        Ticket { previous, done }
    }
}
pub(super) struct Ticket {
    previous: Option<CancellationToken>,
    done: CancellationToken,
}
impl Ticket {
    pub async fn wait(&self) {
        if let Some(previous) = &self.previous {
            previous.cancelled().await;
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.done.cancel();
    }
}

/// Shared transaction/authentication primitives. The caller serializes any
/// usage-specific state updates before releasing the next queued command.
pub(super) async fn send(
    transaction: Transaction,
    credential: Option<&Credential>,
    trace: &Trace,
    next_cseq: impl FnMut() -> crate::Result<u32>,
) -> core::result::Result<Response, OperationError> {
    send_observed(transaction, credential, trace, next_cseq, |_| {}).await
}
pub(super) async fn send_observed(
    mut transaction: Transaction,
    credential: Option<&Credential>,
    trace: &Trace,
    mut next_cseq: impl FnMut() -> crate::Result<u32>,
    mut sending: impl FnMut(&Request),
) -> core::result::Result<Response, OperationError> {
    let mut auth = AuthRetry::new();
    loop {
        sending(&transaction.original);
        trace.state.lock().attempts.push(ExchangeAttempt {
            request: transaction.original.clone(),
            transport_accepted: false,
            response: None,
            response_origin: None,
        });
        transaction
            .send()
            .await
            .map_err(|e| OperationError::Transport(e.to_string()))?;
        if let Some(attempt) = trace.state.lock().attempts.last_mut() {
            attempt.transport_accepted = true;
        }
        let response = loop {
            match transaction.receive().await {
                Some(SipMessage::Response(response))
                    if response.status_code.kind() != StatusCodeKind::Provisional =>
                {
                    break response
                }
                Some(_) => {}
                None => return Err(OperationError::Timeout),
            }
        };
        if let Some(attempt) = trace.state.lock().attempts.last_mut() {
            attempt.response = Some(response.clone());
            attempt.response_origin = Some(transaction.last_response_origin());
        }
        if transaction.last_response_origin() == ResponseOrigin::Local
            && response.status_code == StatusCode::RequestTimeout
        {
            return Err(OperationError::Timeout);
        }
        if matches!(
            response.status_code,
            StatusCode::Unauthorized | StatusCode::ProxyAuthenticationRequired
        ) && auth.should_answer(&response)
        {
            if let Some(credential) = credential {
                transaction = handle_client_authenticate(
                    next_cseq().map_err(protocol)?,
                    &transaction,
                    response,
                    credential,
                )
                .await
                .map_err(protocol)?;
                continue;
            }
        }
        return Ok(response);
    }
}
pub(super) fn protocol(error: impl core::fmt::Display) -> OperationError {
    OperationError::Protocol(error.to_string())
}

/// Header values used by event packages include extension headers such as
/// SIP-ETag. Do not depend on whether the parser knows a typed representation.
pub(super) fn header(message: &impl HeadersExt, name: &str) -> Option<String> {
    message
        .headers()
        .iter()
        .find(|h| h.name().eq_ignore_ascii_case(name))
        .and_then(|h| {
            h.to_string()
                .split_once(':')
                .map(|(_, v)| v.trim().to_string())
        })
}
pub(super) fn token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-.!%*_+`'~".contains(&b))
}

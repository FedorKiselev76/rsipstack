//! Optional re-INVITE retry policy. Each attempt is an ordinary owned operation.
use super::*;
use crate::platform::{self, sync::Mutex, CancellationToken};
use core::{num::NonZeroUsize, time::Duration};

/// Total attempts, including the first. Retries reuse the supplied headers and
/// offer. Enable this only while that modification remains desired; cancellation
/// is explicit and dropping an observer does not cancel it.
#[derive(Clone, Copy, Debug)]
pub struct GlareRetryPolicy {
    pub max_attempts: NonZeroUsize,
}
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GlareRetryError {
    #[error("{0}")]
    Signaling(OperationError),
    #[error("retry admission failed: {0}")]
    Admission(AdmissionError),
    #[error("491 retry limit reached")]
    AttemptsExhausted,
    #[error("re-INVITE retry cancelled")]
    Cancelled,
    #[error("dialog ended before retry")]
    DialogEnded,
    #[error("retry driver stopped")]
    DriverStopped,
}
#[derive(Clone, Debug)]
pub struct GlareRetryCompletion {
    attempts: Vec<OperationCompletion>,
    result: core::result::Result<(), GlareRetryError>,
}
impl GlareRetryCompletion {
    pub fn attempts(&self) -> &[OperationCompletion] {
        &self.attempts
    }
    pub fn response(&self) -> Option<&crate::sip::Response> {
        self.attempts.last().and_then(OperationCompletion::response)
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &GlareRetryError> {
        self.result.as_ref().map(|_| ())
    }
}
struct State {
    attempts: Vec<OperationCompletion>,
    result: Option<core::result::Result<(), GlareRetryError>>,
}
struct Trace {
    id: OperationId,
    state: Mutex<State>,
    done: CancellationToken,
    cancellation: CancellationToken,
}
impl Trace {
    fn finish(&self, result: core::result::Result<(), GlareRetryError>) {
        let mut state = self.state.lock();
        if state.result.is_none() {
            state.result = Some(result);
            self.done.cancel();
        }
    }
}
#[derive(Clone)]
#[must_use]
pub struct ReinviteRetryOperation {
    trace: Arc<Trace>,
}
impl ReinviteRetryOperation {
    /// Identity of the first attempt; each attempt retains its own operation ID.
    pub fn id(&self) -> &OperationId {
        &self.trace.id
    }
    pub fn completed_attempts(&self) -> Vec<OperationCompletion> {
        self.trace.state.lock().attempts.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.trace.done.is_cancelled()
    }
    pub fn request_cancellation(&self) {
        self.trace.cancellation.cancel();
    }
    pub async fn wait(&self) -> GlareRetryCompletion {
        self.trace.done.cancelled().await;
        let state = self.trace.state.lock();
        GlareRetryCompletion {
            attempts: state.attempts.clone(),
            result: state
                .result
                .clone()
                .unwrap_or(Err(GlareRetryError::DriverStopped)),
        }
    }
}
struct DriverGuard(Arc<Trace>);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.finish(Err(GlareRetryError::DriverStopped));
    }
}
impl InviteDialog {
    /// Retry only peer 491 responses, with RFC 3261 section 14.1 jitter.
    /// Admission closure does not cancel the already accepted retry sequence.
    /// Explicit cancellation and dialog termination prevent further attempts.
    pub fn start_reinvite_with_retry(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        options: ReinviteOptions,
        policy: GlareRetryPolicy,
    ) -> core::result::Result<ReinviteRetryOperation, AdmissionError> {
        let _gate = self.1.admit()?;
        let first = DialogOperation(self.0.admit_reinvite(
            headers.clone(),
            body.clone(),
            options.clone(),
        )?);
        let trace = Arc::new(Trace {
            id: first.id().clone(),
            state: Mutex::new(State {
                attempts: Vec::new(),
                result: None,
            }),
            done: CancellationToken::new(),
            cancellation: options.cancellation.clone(),
        });
        let operation = ReinviteRetryOperation {
            trace: trace.clone(),
        };
        let guard = DriverGuard(trace.clone());
        let dialog = self.clone();
        self.1.track(Some(self.id()), "reinvite-retry", async move {
            let _guard = guard;
            trace.finish(drive(&dialog, first, &trace, headers, body, options, policy).await);
        });
        Ok(operation)
    }
    pub fn start_reinvite_body_with_retry(
        &self,
        headers: Option<Vec<Header>>,
        body: Body,
        options: ReinviteOptions,
        policy: GlareRetryPolicy,
    ) -> core::result::Result<ReinviteRetryOperation, AdmissionError> {
        let (headers, body) = body.apply(headers);
        self.start_reinvite_with_retry(headers, body, options, policy)
    }
}
async fn drive(
    dialog: &InviteDialog,
    mut operation: DialogOperation,
    trace: &Trace,
    headers: Option<Vec<Header>>,
    body: Option<Vec<u8>>,
    options: ReinviteOptions,
    policy: GlareRetryPolicy,
) -> core::result::Result<(), GlareRetryError> {
    for attempt in 0..policy.max_attempts.get() {
        let completion = operation.wait().await;
        let retry = completion.response_origin() == Some(ResponseOrigin::Peer)
            && completion
                .response()
                .is_some_and(|r| r.status_code == StatusCode::RequestPending);
        let signaling = completion.signaling_result().map_err(Clone::clone);
        trace.state.lock().attempts.push(completion);
        signaling.map_err(GlareRetryError::Signaling)?;
        if !retry {
            return Ok(());
        }
        if trace.cancellation.is_cancelled() {
            return Err(GlareRetryError::Cancelled);
        }
        if dialog.state().is_terminated() {
            return Err(GlareRetryError::DialogEnded);
        }
        if attempt + 1 == policy.max_attempts.get() {
            return Err(GlareRetryError::AttemptsExhausted);
        }
        let delay = Duration::from_millis(crate::dialog::invite_dialog::glare_backoff_ms(
            dialog.role(),
        ));
        platform::select3(
            core::pin::pin!(platform::sleep(delay)),
            core::pin::pin!(trace.cancellation.cancelled()),
            core::pin::pin!(wait_terminated(dialog)),
        )
        .await;
        if trace.cancellation.is_cancelled() {
            return Err(GlareRetryError::Cancelled);
        }
        if dialog.state().is_terminated() || dialog.0.cancel_token().is_cancelled() {
            return Err(GlareRetryError::DialogEnded);
        }
        operation = DialogOperation(
            dialog
                .0
                .admit_reinvite(headers.clone(), body.clone(), options.clone())
                .map_err(|e| GlareRetryError::Admission(e.into()))?,
        );
    }
    Err(GlareRetryError::AttemptsExhausted)
}

async fn wait_terminated(dialog: &InviteDialog) {
    loop {
        let changed = dialog.0.inner.state_changed.lock().clone();
        if dialog.state().is_terminated() || dialog.0.cancel_token().is_cancelled() {
            return;
        }
        platform::select2(
            core::pin::pin!(changed.cancelled()),
            core::pin::pin!(dialog.0.cancel_token().cancelled()),
        )
        .await;
    }
}

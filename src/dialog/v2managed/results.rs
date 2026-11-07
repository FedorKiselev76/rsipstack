//! Observers expose managed states only, including states retained in results.
use super::*;
use crate::sip::Response;

#[derive(Clone)]
#[must_use]
pub struct DialogOperation(pub(super) crate::dialog::operation::DialogOperation);
impl DialogOperation {
    pub fn id(&self) -> &OperationId {
        self.0.id()
    }
    pub fn attempts(&self) -> Vec<OperationAttempt> {
        self.0.attempts()
    }
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
    pub fn request_cancellation(&self) -> bool {
        self.0.request_cancellation()
    }
    pub async fn wait_first_send(&self) -> core::result::Result<OperationAttempt, OperationError> {
        self.0.wait_first_send().await
    }
    pub async fn wait(&self) -> OperationCompletion {
        OperationCompletion::from_core(self.0.wait().await)
    }
}
#[derive(Clone, Debug)]
pub struct OperationCompletion {
    pub(super) inner: crate::dialog::operation::OperationCompletion,
    state: DialogState,
}
impl OperationCompletion {
    fn from_core(inner: crate::dialog::operation::OperationCompletion) -> Self {
        let state = DialogState::from_core(inner.dialog_state().clone());
        Self { inner, state }
    }
    pub fn id(&self) -> &OperationId {
        self.inner.id()
    }
    pub fn attempts(&self) -> &[OperationAttempt] {
        self.inner.attempts()
    }
    pub fn response(&self) -> Option<&Response> {
        self.inner.response()
    }
    pub fn response_origin(&self) -> Option<ResponseOrigin> {
        self.inner.response_origin()
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        self.inner.signaling_result()
    }
    pub fn dialog_state(&self) -> &DialogState {
        &self.state
    }
}
#[derive(Clone)]
#[must_use]
pub struct InitialInviteOperation(
    pub(super) crate::dialog::initial_operation::InitialInviteOperation,
    pub(super) Arc<control::Control>,
);
impl InitialInviteOperation {
    pub fn operation(&self) -> DialogOperation {
        DialogOperation(self.0.operation())
    }
    pub async fn wait(&self) -> OperationCompletion {
        OperationCompletion::from_core(self.0.wait().await)
    }
    pub fn cancel(&self) -> CancelOperation {
        let operation = self.0.cancel();
        let waiter = operation.clone();
        self.1.track(
            Some(self.0.operation().id().dialog().clone()),
            "cancel",
            async move {
                waiter.wait().await;
            },
        );
        operation
    }
}
#[derive(Clone)]
#[must_use]
pub struct TerminationOperation(pub(super) crate::dialog::termination::TerminationOperation);
impl TerminationOperation {
    pub fn id(&self) -> &OperationId {
        self.0.id()
    }
    pub fn progress(&self) -> TerminationProgress {
        TerminationProgress::from_core(self.0.progress())
    }
    pub async fn wait(&self) -> TerminationCompletion {
        let inner = self.0.wait().await;
        TerminationCompletion {
            state: DialogState::from_core(inner.dialog_state().clone()),
            progress: TerminationProgress::from_core(inner.progress().clone()),
            result: inner.signaling_result().map_err(Clone::clone),
        }
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
impl TerminationProgress {
    fn from_core(value: crate::dialog::termination::TerminationProgress) -> Self {
        Self {
            cancellation: value.cancellation,
            initial_invite: value.initial_invite.map(OperationCompletion::from_core),
            rejection: value.rejection,
            bye: value.bye.map(OperationCompletion::from_core),
        }
    }
}
#[derive(Clone, Debug)]
pub struct TerminationCompletion {
    state: DialogState,
    progress: TerminationProgress,
    result: core::result::Result<(), OperationError>,
}
impl TerminationCompletion {
    pub fn dialog_state(&self) -> &DialogState {
        &self.state
    }
    pub fn progress(&self) -> &TerminationProgress {
        &self.progress
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        self.result.as_ref().map(|_| ())
    }
}

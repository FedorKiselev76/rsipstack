//! Managed events never expose legacy response handles.
pub use super::dialog_layer::Dialog;
use super::*;
pub use crate::dialog::dialog::{
    DialogSnapshot, DialogSnapshotState, ReferStatus, SessionIdState, TerminatedReason,
};
use crate::platform::CancellationToken;

/// Response admission for one incoming request. Observe transmission via its receipt.
#[derive(Clone, Debug)]
pub struct TransactionHandle(pub(super) crate::dialog::dialog::TransactionHandle);
impl TransactionHandle {
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        self.0.start_response(status, headers, body)
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.0.cancellation()
    }
    pub fn finished(&self) -> CancellationToken {
        self.0.finished()
    }
    pub async fn wait_ack(&self) -> crate::Result<Request> {
        self.0.wait_ack().await
    }
}

/// A read-only lifecycle phase, without incoming request or response capabilities.
/// It is an observation, not a promise that a subsequent command will be admitted.
#[derive(Clone, Debug)]
pub enum DialogState {
    Calling(DialogId),
    Trying(DialogId),
    Early(DialogId),
    WaitAck(DialogId),
    Confirmed(DialogId),
    Terminated(DialogId, TerminatedReason),
}
impl DialogState {
    pub fn id(&self) -> &DialogId {
        match self {
            Self::Calling(id)
            | Self::Trying(id)
            | Self::Early(id)
            | Self::WaitAck(id)
            | Self::Confirmed(id)
            | Self::Terminated(id, _) => id,
        }
    }
    pub fn can_cancel(&self) -> bool {
        matches!(self, Self::Calling(_) | Self::Trying(_) | Self::Early(_))
    }
    pub fn is_confirmed(&self) -> bool {
        matches!(self, Self::Confirmed(_))
    }
    pub fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated(..))
    }
    pub fn waiting_ack(&self) -> bool {
        matches!(self, Self::WaitAck(_))
    }
    pub(super) fn from_core(state: crate::dialog::dialog::DialogState) -> Self {
        use crate::dialog::dialog::DialogState as Core;
        match state {
            Core::Calling(id) => Self::Calling(id),
            Core::Trying(id) => Self::Trying(id),
            Core::Early(id, _) => Self::Early(id),
            Core::WaitAck(id, _) => Self::WaitAck(id),
            Core::Confirmed(id, _) => Self::Confirmed(id),
            Core::Terminated(id, reason) => Self::Terminated(id, reason),
            // The shared snapshot representation also classifies request handling
            // as an established phase. The request itself is delivered separately.
            Core::Updated(id, ..)
            | Core::Publish(id, ..)
            | Core::Notify(id, ..)
            | Core::Refer(id, ..)
            | Core::Message(id, ..)
            | Core::Info(id, ..)
            | Core::Options(id, ..) => Self::Confirmed(id),
        }
    }
}
impl core::fmt::Display for DialogState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TransactionHandle {
    pub fn start_response_body(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Body,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        let (headers, body) = body.apply(headers);
        self.start_response(status, headers, body)
    }
}

use super::*;
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdmissionError {
    #[error("new application commands are disabled for this layer")]
    LayerClosed,
    #[error("owned operations require a managed dialog")]
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
impl From<crate::dialog::operation::AdmissionError> for AdmissionError {
    fn from(error: crate::dialog::operation::AdmissionError) -> Self {
        use crate::dialog::operation::AdmissionError as Core;
        match error {
            Core::ExternallyOwnedState => Self::ExternallyOwnedState,
            Core::NotConfirmed => Self::NotConfirmed,
            Core::NegotiationInProgress => Self::NegotiationInProgress,
            Core::Persistence(e) => Self::Persistence(e),
            Core::InvalidRequest(e) => Self::InvalidRequest(e),
        }
    }
}
/// A rejected initial incoming INVITE keeps its transaction available for the
/// application's response policy. Errors after core admission may consume it.
pub struct IncomingInviteError {
    pub error: crate::Error,
    pub transaction: Option<Transaction>,
}
impl core::fmt::Debug for IncomingInviteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IncomingInviteError")
            .field("error", &self.error)
            .field("transaction_retained", &self.transaction.is_some())
            .finish()
    }
}
impl core::fmt::Display for IncomingInviteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.error.fmt(f)
    }
}
impl core::error::Error for IncomingInviteError {}

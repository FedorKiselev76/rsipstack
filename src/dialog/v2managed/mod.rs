#![doc = include_str!("../../../MANAGED-DIALOG-API.md")]
//! Managed dialog API: owned operations, receipt-based responses and private live state.
//!
//! Handles cannot be converted to legacy dialogs or used to mutate live SIP state.
//! Dropping an observer does not cancel signaling; cancellation is explicit.
//!
//! ```compile_fail
//! # async fn wrong(dialog: rsipstack::dialog::v2managed::invite_dialog::InviteDialog) {
//! dialog.bye().await; // Use start_termination instead.
//! # }
//! ```
//! ```compile_fail
//! # fn wrong(dialog: rsipstack::dialog::v2managed::invite_dialog::InviteDialog) {
//! let legacy: rsipstack::dialog::invite_dialog::InviteDialog = dialog.into();
//! # }
//! ```
//! ```compile_fail
//! # fn wrong(dialog: rsipstack::dialog::v2managed::invite_dialog::InviteDialog) {
//! dialog.inner.local_seq.store(99, std::sync::atomic::Ordering::SeqCst);
//! # }
//! ```
//! ```compile_fail
//! # async fn wrong(handle: rsipstack::dialog::v2managed::TransactionHandle) {
//! handle.reply(rsipstack::sip::StatusCode::OK).await;
//! # }
//! ```

//! ```compile_fail
//! # async fn wrong(result: rsipstack::dialog::v2managed::operation::OperationCompletion) {
//! result.dialog_state().start_response(rsipstack::sip::StatusCode::OK, None, None);
//! # }
//! ```
//! ```compile_fail
//! # fn wrong(layer: rsipstack::dialog::v2managed::dialog_layer::DialogLayer) {
//! let mutable_registry = layer.inner;
//! # }
//! ```

#![deny(deprecated)]

pub mod subscription;
pub use subscription::*;
pub mod publication;
pub use publication::*;
pub mod exchange;
pub use exchange::{ExchangeAttempt, ExchangeCompletion, ExchangeOperation};
mod admission;
pub use admission::*;
mod control;
pub use control::{
    DialogDiagnostics, DrainReport, LayerDiagnostics, PendingOperation, PersistenceDrain,
};
pub mod glare;
pub use glare::*;
pub mod body;
pub use body::{Body, InviteOptions};
pub mod events;
pub use events::*;
pub mod dialog;
pub mod dialog_layer;
mod results;
pub use dialog::*;
pub use dialog_layer::*;
pub use results::*;

pub use super::{
    authenticate::Credential,
    dialog::{DialogSnapshot, DialogSnapshotState, ReferStatus, SessionIdState, TerminatedReason},
    incoming::IncomingRequestOperation,
    initial_operation::{CancelCompletion, CancelOperation},
    invitation::InviteOption,
    lifecycle::*,
    operation::{OperationAttempt, OperationError, OperationId, ResponseOrigin},
    persistence::*,
    recovery::{RestoreError, RestoreOutcome},
    reinvite::{AckBody, InviteAnswerHandler, ReinviteOptions},
    reinvite_bridge::{BridgeBodyRole, BridgeDirection, PassthroughSdp, ReinviteBridgePolicy},
    reliable::{
        IncomingPrack, PrackBody, PrackHandler, ReliableProvisionalConfig, ReliableResponse,
    },
    response_operation::{ResponseError, ResponseProgress, ResponseReceipt},
    session_timer::{RefreshMethod, RefreshSdp, SessionTimerConfig, SessionTimerStatus},
    snapshot::*,
    termination::TerminationOptions,
    DialogId,
};
use crate::{
    prelude::*,
    sip::{Header, Method, Request, StatusCode},
    transaction::{key::TransactionRole, Transaction},
};

pub mod invite_dialog;
pub use invite_dialog::*;

pub mod incoming;
pub use incoming::*;

pub mod reinvite_bridge;
pub use reinvite_bridge::*;

/// Completion observers for confirmed requests.
pub mod operation {
    pub use super::{
        AdmissionError, DialogOperation, OperationAttempt, OperationCompletion, OperationError,
        OperationId, ResponseOrigin,
    };
}
/// Initial INVITE and explicit cancellation observers.
pub mod initial_operation {
    pub use super::{CancelCompletion, CancelOperation, InitialInviteOperation};
}
/// Responses report durable completion, transmission, and matching ACK independently.
pub mod response_operation {
    pub use super::{ResponseError, ResponseProgress, ResponseReceipt};
}
/// Phase-independent termination intent and completion.
pub mod termination {
    pub use super::{
        TerminationCompletion, TerminationOperation, TerminationOptions, TerminationProgress,
    };
}
/// Shared immutable call configuration.
pub mod invitation {
    pub use super::{InviteOption, InviteOptions};
}

pub use super::{
    authenticate, lifecycle, persistence, recovery, reliable, session_timer, snapshot,
};
pub mod reinvite {
    pub use super::{AckBody, InviteAnswerHandler, ReinviteOptions};
}

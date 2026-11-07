//! Explicit recovery outcomes. Storage decoding and interrupted SIP replay are
//! application responsibilities. A pending snapshot is returned intact.
use super::{
    dialog::{DialogSnapshot, DialogSnapshotState},
    DialogId,
};
use crate::prelude::*;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RestoreOutcome {
    Restored(DialogId),
    AlreadyPresent(DialogId),
    SkippedNonConfirmed(DialogSnapshotState),
    RecoveryRequired(Box<DialogSnapshot>),
}

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestoreError {
    #[error("{0}")]
    InvalidLifecycle(String),
    #[error("snapshot initialization failed: {0}")]
    Initialization(String),
}

//! Owned incoming INVITE exchange and explicit logical UAS branches.
//!
//! A B2BUA may expose independent early dialogs for different downstream offers.
//! Each branch has its own identity and sequence state. The incoming exchange
//! admits one final response; the selected branch survives, the others retire.
//! The stack owns transaction dispatch, CANCEL, response retransmission and ACK.
use super::{
    dialog::{Dialog, DialogInner, DialogState, DialogStateSender, TerminatedReason},
    dialog_layer::{DialogLayer, DialogLayerInner},
    invite_dialog::InviteDialog,
    persistence::DialogCheckpointReason,
    response_operation::{ResponseError, ResponseReceipt, ResponseSubmission},
    DialogId,
};
use crate::{
    platform::{self, sync::Mutex, CancellationToken},
    prelude::*,
    sip::{HeadersExt, Method, Response, SipMessage, StatusCode, StatusCodeKind, Uri},
    transaction::{key::TransactionRole, make_tag, transaction::Transaction},
};
use alloc::sync::Weak;

#[derive(Clone)]
pub struct IncomingInvite {
    inner: Arc<IncomingInviteInner>,
}
pub(crate) struct IncomingInviteInner {
    layer: Weak<DialogLayerInner>,
    branches: Mutex<Vec<InviteDialog>>,
    // Updated under shared response admission, before its command can race with
    // another branch's BYE. Branch creation alone does not select that branch.
    selected: Mutex<Weak<DialogInner>>,
    finished: CancellationToken,
    cancellation: CancellationToken,
    result: Mutex<Option<core::result::Result<(), super::operation::OperationError>>>,
}
struct IncomingInviteGuard(Arc<IncomingInviteInner>);
impl Drop for IncomingInviteGuard {
    fn drop(&mut self) {
        let mut result = self.0.result.lock();
        if result.is_none() {
            *result = Some(Err(super::operation::OperationError::DriverStopped));
        }
        self.0.finished.cancel();
    }
}
impl IncomingInvite {
    /// Wait for the initial exchange's signaling outcome. Dropping this waiter
    /// leaves the driver and any retransmission cleanup running.
    pub async fn wait(&self) -> core::result::Result<(), super::operation::OperationError> {
        self.inner.finished.cancelled().await;
        self.inner
            .result
            .lock()
            .clone()
            .unwrap_or(Err(super::operation::OperationError::DriverStopped))
    }
    /// Notified when a matching CANCEL is received, before the final response.
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }
    /// The initial logical UAS. Applications that do not fork use this dialog.
    pub fn dialog(&self) -> InviteDialog {
        self.inner.branches.lock()[0].clone()
    }

    /// Create another logical UAS for a different early offer/answer exchange.
    /// The stack allocates its To-tag; the application never rewrites identity.
    /// Branching with 100rel/session timers is deliberately rejected until those
    /// extension negotiations can be coordinated across the incoming exchange.
    pub fn branch(&self) -> core::result::Result<InviteDialog, ResponseError> {
        let root = self.dialog();
        root.inner.initial_responses.submit(&StatusCode::Trying, || {
            if self.inner.finished.is_cancelled() {
                return Err(ResponseError::Closed);
            }
            let layer = self.inner.layer.upgrade().ok_or(ResponseError::Closed)?;
            if root.inner.reliable_config.lock().is_some()
                || root.inner.session_timer.lock().is_some()
            {
                return Err(ResponseError::Protocol("early branching with reliable provisionals or session timers is unsupported".into()));
            }
            let mut branches = self.inner.branches.lock();
            if branches.len() >= 64 {
                return Err(ResponseError::Overloaded);
            }
            let id = DialogId {
                local_tag: make_tag().to_string(),
                ..root.id()
            };
            let mut inner = DialogInner::new(
                TransactionRole::Server,
                id.clone(),
                root.initial_request(),
                root.inner.endpoint_inner.clone(),
                root.inner.state_sender.clone(),
                root.inner.credential.clone(),
                root.inner.local_contact.clone(),
                root.inner.tu_sender.clone(),
            ).map_err(|e| ResponseError::Protocol(e.to_string()))?;
            inner.owned_state = true;
            inner.admit_remote_seq(root.initial_request().cseq_header().map_err(|e| ResponseError::Protocol(e.to_string()))?.seq().map_err(|e| ResponseError::Protocol(e.to_string()))?, &Method::Invite)
                .map_err(|_| ResponseError::Protocol("invalid initial CSeq".into()))?;
            inner.initial_responses = root.inner.initial_responses.clone();
            *inner.remote_contact.lock() = root.initial_request().contact_header().ok().cloned();
            *inner.incoming_invite.lock() = Arc::downgrade(&self.inner);
            inner.lifecycle.lock().sender = layer.lifecycle_sender.lock().clone();
            let branch = InviteDialog::from_inner(Arc::new(inner));
            branch.inner.configure_persistence(layer.persistence_config.lock().clone());
            branch.inner.set_server_connection(root.inner.server_connection.lock().clone());
            layer.dialogs.insert(id, Dialog::Invite(branch.clone()));
            branches.push(branch.clone());
            Ok(branch)
        })
    }
}
impl IncomingInviteInner {
    pub(crate) fn select_response(&self, selected: &Arc<DialogInner>) {
        *self.selected.lock() = Arc::downgrade(selected);
    }

    pub(crate) fn finish_early_bye(
        &self,
        ended: &Arc<DialogInner>,
    ) -> core::result::Result<Option<ResponseReceipt>, ResponseError> {
        ended.initial_responses.submit_final_if(|| {
            if !self
                .selected
                .lock()
                .upgrade()
                .is_some_and(|selected| Arc::ptr_eq(&selected, ended))
            {
                // BYE for an older logical early branch must not reject the
                // active branch's INVITE exchange.
                return Ok(None);
            }
            let request = ended.initial_request.lock().clone();
            let response = ended.make_response(&request, StatusCode::RequestTerminated, None, None);
            let (receipt, producer) = ResponseReceipt::pair();
            ended
                .tu_sender
                .send(
                    ResponseSubmission {
                        response,
                        producer: Some(producer),
                        initial_owner: Some(Arc::downgrade(ended)),
                    }
                    .event(),
                )
                .map_err(|_| ResponseError::Closed)?;
            Ok(Some(receipt))
        })
    }

    pub(crate) fn final_response(&self, selected: &Arc<DialogInner>, response: &Response) {
        if response.status_code.kind() == StatusCodeKind::Provisional {
            return;
        }
        let branches = self.branches.lock().clone();
        for branch in branches {
            if !Arc::ptr_eq(&branch.inner, selected) {
                branch.inner.transition(DialogState::Terminated(
                    branch.id(),
                    TerminatedReason::UasOther(StatusCode::RequestTerminated),
                ));
                if let Some(layer) = self.layer.upgrade() {
                    layer.dialogs.remove(&branch.id());
                }
            }
        }
    }
}
impl DialogLayer {
    /// Consume an initial server INVITE and own its complete transaction driver.
    /// A dropped handle does not cancel the call. Answer using the dialog's
    /// `start_response`; end it using `start_termination`.
    /// A valid early BYE for the active response branch also queues 487 for an
    /// unanswered INVITE. An already admitted final response wins. BYE for a
    /// previous logical branch does not reject the active branch's exchange.
    pub fn start_incoming_invite(
        self: &Arc<Self>,
        mut transaction: Transaction,
        states: DialogStateSender,
        contact: Option<Uri>,
    ) -> crate::Result<IncomingInvite> {
        if transaction.role() != TransactionRole::Server
            || transaction.original.method != Method::Invite
            || transaction.original.to_header()?.tag()?.is_some()
        {
            return Err(crate::Error::Error("expected an initial INVITE".into()));
        }
        let root = self.get_or_create_server_invite(&transaction, states, None, contact)?;
        root.inner
            .admit_remote_seq(transaction.original.cseq_header()?.seq()?, &Method::Invite)
            .map_err(|_| crate::Error::Error("invalid initial CSeq".into()))?;
        let inner = Arc::new(IncomingInviteInner {
            layer: Arc::downgrade(&self.inner),
            branches: Mutex::new(vec![root.clone()]),
            selected: Mutex::new(Arc::downgrade(&root.inner)),
            finished: CancellationToken::new(),
            cancellation: CancellationToken::new(),
            result: Mutex::new(None),
        });
        *root.inner.incoming_invite.lock() = Arc::downgrade(&inner);
        let driver = inner.clone();
        platform::spawn(async move {
            let _guard = IncomingInviteGuard(driver.clone());
            let result = async {
                if root.inner.reject_required_100rel(&mut transaction).await?
                    || root.inner.handle_session_timer(&mut transaction).await?
                {
                    return Ok(());
                }
                root.inner.transition_if_calling();
                transaction.send_trying().await?;
                while let Some(message) = transaction.receive().await {
                    let active = transaction
                        .initial_dialog
                        .lock()
                        .as_ref()
                        .and_then(Weak::upgrade)
                        .unwrap_or_else(|| root.inner.clone());
                    if let SipMessage::Request(request) = message {
                        match request.method {
                            Method::Ack => {
                                if !active.is_terminated() {
                                    active.remote_ack.lock().replace(request);
                                    let id = active.id.lock().clone();
                                    active.transition(DialogState::Confirmed(
                                        id,
                                        transaction.last_response.clone().unwrap_or_default(),
                                    ));
                                }
                                break;
                            }
                            Method::Cancel => {
                                driver.cancellation.cancel();
                                transaction.reply(StatusCode::RequestTerminated).await?;
                                let id = active.id.lock().clone();
                                active.transition(DialogState::Terminated(
                                    id,
                                    TerminatedReason::UacCancel,
                                ));
                                if let Some(response) = &transaction.last_response {
                                    driver.final_response(&active, response);
                                }
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                transaction.finish_ack_dispatch();
                transaction.check_persistence()?;
                let active = transaction
                    .initial_dialog
                    .lock()
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| root.inner.clone());
                let answered = active.waiting_ack();
                active.end_session_without_ack(&transaction, answered).await;
                active
                    .checkpoint(DialogCheckpointReason::SignalingCompleted, None)
                    .await?;
                Ok::<_, crate::Error>(())
            }
            .await;
            if let Err(error) = &result {
                tracing::warn!(%error, "incoming INVITE driver failed");
            }
            *driver.result.lock() =
                Some(result.map_err(|e| super::operation::OperationError::Protocol(e.to_string())));
            driver.finished.cancel();
            while transaction.receive().await.is_some() {}
        });
        Ok(IncomingInvite { inner })
    }
}

/// Observer for a stack-owned incoming dialog request. Dropping the observer
/// does not cancel response processing or transaction retransmission timers.
#[derive(Clone)]
#[must_use]
pub struct IncomingRequestOperation {
    inner: Arc<IncomingRequestTrace>,
}
struct IncomingRequestTrace {
    done: CancellationToken,
    result: Mutex<Option<core::result::Result<(), super::operation::OperationError>>>,
}
impl IncomingRequestOperation {
    pub async fn wait(&self) -> core::result::Result<(), super::operation::OperationError> {
        self.inner.done.cancelled().await;
        self.inner
            .result
            .lock()
            .clone()
            .unwrap_or(Err(super::operation::OperationError::DriverStopped))
    }
}
struct IncomingRequestGuard(Arc<IncomingRequestTrace>);
impl Drop for IncomingRequestGuard {
    fn drop(&mut self) {
        self.0.done.cancel();
    }
}
impl InviteDialog {
    /// Admit a matched incoming request in receive order, then own its driver.
    /// Sequence admission runs synchronously, so independently scheduled request
    /// handlers cannot reorder a burst of INFO/OPTIONS before validating CSeq.
    /// Application response policy is delivered through the dialog state channel.
    pub fn start_incoming_request(
        &self,
        mut transaction: Transaction,
    ) -> crate::Result<IncomingRequestOperation> {
        if transaction.role() != TransactionRole::Server
            || DialogId::try_from((&transaction.original, TransactionRole::Server))? != self.id()
        {
            return Err(crate::Error::Error(
                "expected a matching in-dialog request transaction".into(),
            ));
        }
        let admitted = matches!(transaction.original.method, Method::Ack | Method::Cancel)
            || self
                .inner
                .admit_remote_seq(
                    transaction.original.cseq_header()?.seq()?,
                    &transaction.original.method,
                )
                .is_ok();
        let trace = Arc::new(IncomingRequestTrace {
            done: CancellationToken::new(),
            result: Mutex::new(None),
        });
        let operation = IncomingRequestOperation {
            inner: trace.clone(),
        };
        let id = self.id();
        let previous_acks = self
            .inner
            .endpoint_inner
            .pending_ack_dispatch
            .with(|pending| {
                pending
                    .iter()
                    .filter(|((dialog, _), _)| *dialog == id)
                    .map(|(_, token)| token.clone())
                    .collect::<Vec<_>>()
            });
        let guard = IncomingRequestGuard(trace.clone());
        let mut dialog = self.clone();
        platform::spawn(async move {
            let _guard = guard;
            for ack in previous_acks {
                ack.cancelled().await;
            }
            let result = if admitted {
                dialog.handle_admitted(&mut transaction, true).await
            } else {
                transaction.reply(StatusCode::ServerInternalError).await
            };
            *trace.result.lock() =
                Some(result.map_err(|e| super::operation::OperationError::Protocol(e.to_string())));
            trace.done.cancel();
            while transaction.receive().await.is_some() {}
        });
        Ok(operation)
    }
}

impl Dialog {
    /// Transfer an incoming in-dialog transaction to its stack-owned driver.
    pub fn start_incoming_request(
        &self,
        mut transaction: Transaction,
    ) -> crate::Result<IncomingRequestOperation> {
        if let Self::Invite(dialog) = self {
            return dialog.start_incoming_request(transaction);
        }
        if transaction.role() != TransactionRole::Server
            || DialogId::try_from((&transaction.original, TransactionRole::Server))? != self.id()
        {
            return Err(crate::Error::Error(
                "expected a matching dialog request".into(),
            ));
        }
        let trace = Arc::new(IncomingRequestTrace {
            done: CancellationToken::new(),
            result: Mutex::new(None),
        });
        let operation = IncomingRequestOperation {
            inner: trace.clone(),
        };
        let mut dialog = self.clone();
        let guard = IncomingRequestGuard(trace.clone());
        platform::spawn(async move {
            let _guard = guard;
            let result = match &mut dialog {
                Dialog::Invite(dialog) => dialog.handle_admitted(&mut transaction, false).await,
                Dialog::ServerSubscription(dialog) => dialog.handle(&mut transaction).await,
                Dialog::ClientSubscription(dialog) => dialog.handle(&mut transaction).await,
                Dialog::ServerPublication(dialog) => dialog.handle(&mut transaction).await,
                Dialog::ClientPublication(dialog) => dialog.handle(&mut transaction).await,
            };
            *trace.result.lock() =
                Some(result.map_err(|e| super::operation::OperationError::Protocol(e.to_string())));
            trace.done.cancel();
            while transaction.receive().await.is_some() {}
        });
        Ok(operation)
    }
}

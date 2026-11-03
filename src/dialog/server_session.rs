//! Incoming INVITE with sequential early dialogs (for a B2BUA hunting group).
//!
//! Each early answer has its own immutable dialog identity. The session owns
//! the common INVITE transaction; only the selected 2xx dialog survives ACK.
use super::{
    dialog::{Dialog, DialogInner, DialogState, DialogStateSender, TerminatedReason},
    dialog_layer::DialogLayer,
    invite_dialog::InviteDialog,
    DialogId,
};
use crate::{
    platform::{sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, HeadersExt, Method, Request, SipMessage, StatusCode, StatusCodeKind, Uri},
    transaction::{
        key::TransactionRole,
        transaction::{Transaction, TransactionEvent},
    },
    Result,
};

struct SessionInner {
    layer: Arc<DialogLayer>,
    original: Request,
    connection: Option<crate::transport::SipConnection>,
    sender: crate::transaction::transaction::TransactionEventSender,
    states: DialogStateSender,
    contact: Uri,
    branches: Mutex<Vec<InviteDialog>>,
    selected: Mutex<Option<InviteDialog>>,
    finished: Mutex<bool>,
    respond_lock: Mutex<()>,
    cancelled: CancellationToken,
}

/// Controller for an incoming INVITE whose B2BUA may try several destinations.
/// No routing policy lives here. The application chooses the branch tag and
/// response; the stack owns each dialog, ACK matching and transaction lifetime.
#[derive(Clone)]
pub struct ServerInviteSession(Arc<SessionInner>);

impl DialogLayer {
    /// Start driving an initial INVITE independently of application policy work.
    pub fn server_invite_session(
        self: &Arc<Self>,
        mut tx: Transaction,
        states: DialogStateSender,
        contact: Uri,
    ) -> Result<ServerInviteSession> {
        if tx.original.method != Method::Invite || tx.original.to_header()?.tag()?.is_some() {
            return Err(crate::Error::Error("expected an initial INVITE".into()));
        }
        let session = ServerInviteSession(Arc::new(SessionInner {
            layer: self.clone(),
            original: tx.original.clone(),
            connection: tx.connection.clone(),
            sender: tx.tu_sender.clone(),
            states,
            contact,
            branches: Mutex::new(Vec::new()),
            selected: Mutex::new(None),
            finished: Mutex::new(false),
            respond_lock: Mutex::new(()),
            cancelled: CancellationToken::new(),
        }));
        let driver = session.clone();
        crate::platform::spawn(async move {
            let _ = tx.send_trying().await;
            while let Some(message) = tx.receive().await {
                if let SipMessage::Request(req) = message {
                    match req.method {
                        Method::Cancel => {
                            {
                                let _serial = driver.0.respond_lock.lock();
                                let mut finished = driver.0.finished.lock();
                                if *finished {
                                    continue;
                                }
                                *finished = true;
                            }
                            driver.0.cancelled.cancel();
                            let _ = tx.reply(StatusCode::RequestTerminated).await;
                            let branches = driver.0.branches.lock().clone();
                            for branch in branches {
                                branch.inner.transition(DialogState::Terminated(
                                    branch.id(),
                                    TerminatedReason::UacCancel,
                                ));
                            }
                        }
                        Method::Ack => {
                            let selected = driver.0.selected.lock().clone();
                            if let Some(branch) = selected {
                                if DialogId::try_from((&req, TransactionRole::Server))
                                    .ok()
                                    .as_ref()
                                    == Some(&branch.id())
                                    && req.cseq_header().and_then(|c| c.seq()).ok()
                                        == driver
                                            .0
                                            .original
                                            .cseq_header()
                                            .and_then(|c| c.seq())
                                            .ok()
                                {
                                    branch.inner.remote_ack.lock().replace(req);
                                    branch.inner.transition(DialogState::Confirmed(
                                        branch.id(),
                                        tx.last_response.clone().unwrap_or_default(),
                                    ));
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            let selected = driver.0.selected.lock().clone();
            if let Some(branch) = selected {
                branch
                    .inner
                    .end_session_without_ack(&tx, branch.inner.waiting_ack())
                    .await;
            }
        });
        Ok(session)
    }
}

impl ServerInviteSession {
    pub fn cancelled(&self) -> CancellationToken {
        self.0.cancelled.clone()
    }

    /// Select/create an early branch. Tags must be SIP tokens and remain stable
    /// for the life of a branch. A confirmed dialog can never be retagged.
    pub fn branch(&self, local_tag: &str) -> Result<InviteDialog> {
        let mut branches = self.0.branches.lock();
        if let Some(branch) = branches.iter().find(|b| b.id().local_tag == local_tag) {
            return Ok(branch.clone());
        }
        if *self.0.finished.lock()
            || local_tag.is_empty()
            || !local_tag
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-.!%*_+`'~".contains(&c))
        {
            return Err(crate::Error::Error(
                "invalid or completed INVITE branch".into(),
            ));
        }
        let mut id = DialogId::try_from((&self.0.original, TransactionRole::Server))?;
        id.local_tag = local_tag.into();
        let inner = Arc::new(DialogInner::new(
            TransactionRole::Server,
            id.clone(),
            self.0.original.clone(),
            self.0.layer.endpoint.clone(),
            self.0.states.clone(),
            None,
            Some(self.0.contact.clone()),
            self.0.sender.clone(),
        )?);
        *inner.remote_contact.lock() = self.0.original.contact_header().ok().cloned();
        inner.remote_seq.store(
            self.0.original.cseq_header()?.seq()?,
            core::sync::atomic::Ordering::Relaxed,
        );
        inner.set_server_connection(self.0.connection.clone());
        let branch = InviteDialog::from_inner(inner);
        self.0
            .layer
            .inner
            .dialogs
            .insert(id.to_string(), Dialog::Invite(branch.clone()));
        branches.push(branch.clone());
        Ok(branch)
    }

    /// Respond on an early branch. Identity and routing headers are generated
    /// by the stack; supplied headers are application payload metadata only.
    pub fn respond(
        &self,
        local_tag: &str,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<()> {
        let _serial = self.0.respond_lock.lock();
        if status == StatusCode::Trying {
            return Ok(());
        }
        if *self.0.finished.lock() {
            return Ok(());
        }
        let branch = self.branch(local_tag)?;
        let response = branch
            .inner
            .make_response(&self.0.original, status.clone(), headers, body);
        let state = match status.kind() {
            StatusCodeKind::Provisional => DialogState::Early(branch.id(), response.clone()),
            StatusCodeKind::Successful => {
                *self.0.selected.lock() = Some(branch.clone());
                // Selecting a final answer also retires earlier attempts. Their
                // tags must never route a stray ACK into this INVITE transaction.
                for other in self.0.branches.lock().iter() {
                    if other.id() != branch.id() {
                        other.inner.transition(DialogState::Terminated(
                            other.id(),
                            TerminatedReason::UasOther(StatusCode::RequestTerminated),
                        ));
                        self.0.layer.remove_dialog(&other.id());
                    }
                }
                *self.0.finished.lock() = true;
                DialogState::WaitAck(branch.id(), response.clone())
            }
            _ => {
                *self.0.finished.lock() = true;
                DialogState::Terminated(branch.id(), TerminatedReason::UasOther(status))
            }
        };
        self.0.sender.send(TransactionEvent::Respond(response))?;
        branch.inner.transition(state);
        Ok(())
    }

    pub fn dialogs(&self) -> Vec<InviteDialog> {
        self.0.branches.lock().clone()
    }
}

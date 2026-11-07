//! Shared INVITE session implementation and compatibility type.
//!
//! Existing import paths are preserved. New applications use the encapsulated
//! [`crate::dialog::v2managed::invite_dialog::InviteDialog`] facade; legacy
//! borrowed signaling methods are implemented in the compatibility adapter.

use super::dialog::DialogInner;
use super::dialog::InviteAck;
use super::dialog::{Dialog, DialogInnerRef, DialogState, TerminatedReason, TransactionHandle};
use super::subscription::{ClientSubscriptionDialog, ServerSubscriptionDialog};
use super::DialogId;
use crate::platform::CancellationToken;
use crate::prelude::*;
use crate::sip::prelude::{HasHeaders, HeadersExt};
use crate::sip::{Header, Method, Request, Response, SipMessage, StatusCode, StatusCodeKind};
use crate::transaction::key::TransactionRole;
use crate::transaction::transaction::{Transaction, TransactionEvent};
use crate::Result;
use core::sync::atomic::Ordering;
use tracing::{debug, info, trace, warn};

/// RFC 3261 section 14.1 gives the Call-ID owner and its peer disjoint
/// retry windows, in 10 ms units. The dialog role identifies that owner,
/// regardless of which party is sending this particular re-INVITE.
pub(super) fn glare_backoff_ms(role: TransactionRole) -> u64 {
    let mut bytes = [0u8; 8];
    crate::transaction::fill_random_bytes(&mut bytes);
    let random = u64::from_le_bytes(bytes);
    match role {
        TransactionRole::Client => (210 + random % 191) * 10,
        TransactionRole::Server => (random % 201) * 10,
    }
}

/// Unified INVITE dialog that can act as either a UAS (Server) or UAC (Client).
///
/// Use `start_request`, `start_reinvite`, `start_response`,
/// `start_incoming_request` and `start_termination` for signaling. Legacy
/// borrowed-future methods remain deprecated for migration. Applications can
/// use `#![deny(deprecated)]` to prohibit accidental fallback.
///
/// ```compile_fail
/// #![deny(deprecated)]
/// use rsipstack::dialog::invite_dialog::InviteDialog;
/// async fn legacy_call(dialog: &InviteDialog) {
///     let _ = dialog.bye().await;
/// }
/// ```
#[derive(Clone)]
pub struct InviteDialog {
    pub(super) inner: DialogInnerRef,
}

// Wake local teardown if the INVITE driver is dropped during ACK. It must
// report an interrupted ACK instead of waiting forever or sending BYE first.
struct InitialAckGuard(Arc<InviteAck>);

impl Drop for InitialAckGuard {
    fn drop(&mut self) {
        self.0.done.cancel();
    }
}

impl InviteDialog {
    pub fn from_inner(inner: DialogInnerRef) -> Self {
        Self { inner }
    }
    /// The dialog role (Client = UAC we initiated, Server = UAS we received).
    pub fn role(&self) -> TransactionRole {
        self.inner.role
    }

    pub fn id(&self) -> DialogId {
        self.inner.id.lock().clone()
    }

    pub fn state(&self) -> DialogState {
        self.inner.state.lock().clone()
    }

    /// Inspect checkpoint progress and any permanent storage failure.
    pub fn persistence_status(&self) -> Option<super::persistence::DialogPersistenceStatus> {
        self.inner
            .persistence
            .lock()
            .as_ref()
            .map(|runtime| runtime.status.lock().clone())
    }

    /// Capture a cutoff of accepted persistence work. Later writes do not extend
    /// it. A storage callback must not await its own dialog's barrier.
    pub fn persistence_barrier(&self) -> super::persistence::PersistenceBarrier {
        self.inner.persistence_barrier()
    }

    pub fn snapshot(&self) -> super::dialog::DialogSnapshot {
        self.inner.snapshot()
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.inner.cancel_token
    }

    /// The most recent ACK received for an INVITE or re-INVITE this dialog
    /// answered (the whole request, headers and body), or `None` if none has
    /// been received since the dialog was created or restored.
    ///
    /// When the INVITE or re-INVITE carried no offer, the offer goes in the
    /// 2xx and the answer comes back in the ACK body (RFC 3261 §13.2.1,
    /// §14.2). The ACK is recorded when the INVITE transaction delivers it,
    /// before the `Confirmed` state it causes is notified. Each such ACK
    /// replaces the previous one; a new re-INVITE does not clear it, so until
    /// its ACK arrives this still returns the previous one. `Confirmed` is
    /// also notified after other mid-dialog requests: match the ACK's CSeq
    /// number against the INVITE it should acknowledge.
    pub fn last_remote_ack(&self) -> Option<Request> {
        self.inner.remote_ack.lock().clone()
    }

    /// Enable full 100rel before starting the dialog. Prefer configuring the layer
    /// before call creation; changes during an INVITE exchange are unsupported.
    pub fn set_reliable_provisional(
        &self,
        config: Option<super::reliable::ReliableProvisionalConfig>,
    ) {
        *self.inner.reliable_config.lock() = config;
    }

    /// The initial INVITE request that created this dialog.
    pub fn initial_request(&self) -> Request {
        self.inner.initial_request.lock().clone()
    }

    // ── UAS (Server) semantics ────────────────────────────────────────────

    /// Submit a response to the original incoming INVITE. The owning dialog
    /// handler must remain running. Dropping the receipt does not cancel the
    /// command; its send and matching ACK can be observed independently.
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<
        super::response_operation::ResponseReceipt,
        super::response_operation::ResponseError,
    > {
        use super::response_operation::{ResponseError, ResponseReceipt, ResponseSubmission};
        if self.role() != TransactionRole::Server {
            return Err(ResponseError::Protocol(
                "only an incoming INVITE can be answered".into(),
            ));
        }
        if !self.inner.can_cancel() {
            return Err(ResponseError::Protocol(
                "initial INVITE no longer accepts responses".into(),
            ));
        }
        let (receipt, producer) = ResponseReceipt::pair();
        let selects_branch = status != StatusCode::Trying;
        let response = self
            .inner
            .make_response(&self.initial_request(), status, headers, body);
        self.inner
            .initial_responses
            .submit(&response.status_code.clone(), || {
                self.inner
                    .tu_sender
                    .send(
                        ResponseSubmission {
                            response,
                            producer: Some(producer),
                            initial_owner: Some(Arc::downgrade(&self.inner)),
                        }
                        .event(),
                    )
                    .map_err(|_| ResponseError::Closed)?;
                if selects_branch {
                    if let Some(incoming) = self.inner.incoming_invite.lock().upgrade() {
                        incoming.select_response(&self.inner);
                    }
                }
                Ok(())
            })?;
        Ok(receipt)
    }

    // ── UAC (Client) semantics ────────────────────────────────────────────

    /// Admit cancellation of an initial INVITE created with
    /// `DialogLayer::start_invite`. The returned receipt observes CANCEL
    /// separately from the INVITE, which may still complete successfully.
    pub fn start_cancel(&self) -> Result<super::initial_operation::CancelOperation> {
        let control = self.inner.initial_operation.lock().upgrade();
        if let Some(control) = control {
            return Ok(control.cancel());
        }
        if self.inner.is_confirmed() || self.inner.is_terminated() {
            return Ok(super::initial_operation::CancelOperation::already_final());
        }
        Err(crate::Error::Error(
            "initial cancellation requires an INVITE admitted through start_invite".into(),
        ))
    }

    /// Send the CANCEL `cancel()` deferred in Calling, once the INVITE got a
    /// provisional response. Skipped when a final response was processed
    /// before the task ran.
    pub(super) fn send_deferred_cancel(&self) {
        if !self.inner.cancel_deferred.swap(false, Ordering::SeqCst) {
            return;
        }
        let dialog = self.clone();
        let _handle = crate::platform::spawn(async move {
            if !dialog.inner.can_cancel() {
                return;
            }
            if let Err(e) = dialog.send_cancel().await {
                warn!(id = %dialog.id(), error = %e, "deferred cancel failed");
            }
        });
    }

    /// End the session a 2xx to a cancelled INVITE established: the CANCEL
    /// had no effect on it (RFC 3261 §9.1, §15). The transaction ACKs the
    /// 2xx (unless `auto_ack_2xx` is off), then a BYE follows.
    pub(super) fn bye_2xx_of_cancelled(&self, resp: Response) {
        let dialog = self.clone();
        let _handle = crate::platform::spawn(async move {
            super::invitation::bye_if_2xx(&dialog, Some(resp)).await;
        });
    }

    /// Send the CANCEL for the initial INVITE, whatever the dialog state.
    pub(super) async fn send_cancel(&self) -> Result<()> {
        debug!(id = %self.id(), "sending cancel request");
        let mut cancel_request = self.inner.initial_request.lock().clone();
        let invite_seq = cancel_request.cseq_header()?.seq()?;
        cancel_request
            .headers_mut()
            .retain(|h| !matches!(h, Header::ContentLength(_) | Header::ContentType(_)));

        cancel_request.method = Method::Cancel;
        cancel_request
            .cseq_header_mut()?
            .mut_seq(invite_seq)?
            .mut_method(Method::Cancel)?;
        cancel_request.body = vec![];
        let Some(response) = self.inner.do_request(cancel_request).await? else {
            return Ok(());
        };
        let status = response.status_code;
        if status.kind() != StatusCodeKind::Successful {
            return Err(crate::Error::DialogError(
                format!("CANCEL failed with response {status}"),
                self.id(),
                status,
            ));
        }
        Ok(())
    }

    pub(super) async fn process_invite_with_registration(
        &self,
        tx: &mut Transaction,
        mut register: impl FnMut(&DialogId) + Send,
    ) -> Result<(DialogId, Option<Response>)> {
        if self.role() != TransactionRole::Client {
            return Err(crate::Error::DialogError(
                "process_invite requires a UAC (Client) dialog".to_string(),
                self.id(),
                StatusCode::ServerInternalError,
            ));
        }
        use super::authenticate::{handle_client_authenticate, AuthRetry};

        self.inner.reserve_invite(tx)?;
        self.inner.transition(DialogState::Calling(self.id()));
        let mut auth = AuthRetry::new();
        self.inner.bind_persistence(tx);
        tx.track_receipt_times(
            self.inner.lifecycle.lock().sender.is_some() || self.inner.persistence.lock().is_some(),
        );
        let mut timer_retries = 0;
        let initial_control = self.inner.initial_operation.lock().upgrade();
        if let Some(control) = &initial_control {
            if !control.begin_attempt() {
                control.suppressed.store(true, Ordering::SeqCst);
                control
                    .trace
                    .failed(super::operation::OperationError::CancelledBeforeSend);
                self.inner.transition(DialogState::Terminated(
                    self.id(),
                    TerminatedReason::UacCancel,
                ));
                return Ok((self.id(), None));
            }
        }
        tx.send().await?;
        // Record the flow the INVITE actually went out on, so later
        // in-dialog requests (BYE / re-INVITE / INFO) reuse it instead of
        // destination-routing off a possibly unroutable Contact.
        self.inner.set_server_connection(tx.connection.clone());
        let mut dialog_id = self.id();
        let mut final_response = None;
        loop {
            let ack = InviteAck::new();
            let _ack_guard = InitialAckGuard(ack.clone());
            let mut established = Ok(false);
            let timer = self.inner.session_timer.lock().clone();
            let request = tx.original.clone();
            let mut timer_result = Ok(());
            let msg = tx
                .receive_with_response_observer(|resp, received_at| {
                    if let Some(control) = &initial_control {
                        control.observe_response(resp);
                    }
                    let peer_uuid = resp.session_id_header().and_then(|s| s.local_uuid());
                    self.inner.observe_peer_session_uuid(peer_uuid);
                    if resp.status_code.kind() == StatusCodeKind::Successful {
                        self.inner.initial_ack.lock().replace(ack.clone());
                        established = self.establish_from_2xx(resp, received_at, &mut register);
                        if established.is_ok() {
                            if let Some(timer) = &timer {
                                timer_result = timer.received(&request, resp);
                            }
                        }
                    }
                })
                .await;
            tx.check_persistence()?;
            ack.finish();
            if msg.is_some() {
                self.inner.complete_initial_ack();
                if self.inner.is_confirmed() || self.inner.is_terminated() {
                    self.inner
                        .checkpoint(
                            super::persistence::DialogCheckpointReason::SignalingCompleted,
                            None,
                        )
                        .await?;
                }
            }
            let cancelled = established?;
            if let Err(error) = timer_result {
                self.send_bye_headers(None).await.ok();
                return Err(error);
            }
            let Some(msg) = msg else { break };
            if matches!(&msg, SipMessage::Response(r) if r.status_code.kind() != StatusCodeKind::Provisional)
            {
                tx.track_receipt_times(false);
            }
            match msg {
                SipMessage::Request(_) => {}
                SipMessage::Response(resp) => {
                    let status = resp.status_code.clone();
                    if status == StatusCode::Trying {
                        self.inner.transition(DialogState::Trying(self.id()));
                        self.send_deferred_cancel();
                        continue;
                    }

                    if matches!(status.kind(), crate::sip::StatusCodeKind::Provisional) {
                        // Not held back by a PRACK (RFC 3262); again after
                        // the transition for a cancel() racing this one.
                        self.send_deferred_cancel();
                        if !self
                            .inner
                            .handle_provisional_response(&resp, &tx.original)
                            .await?
                        {
                            continue;
                        }
                        self.inner.transition(DialogState::Early(self.id(), resp));
                        self.send_deferred_cancel();
                        continue;
                    }

                    // A challenge to a cancelled INVITE is its final response.
                    self.inner
                        .clear_remote_reliable(tx.original.cseq_header()?.seq()?);
                    if matches!(
                        status,
                        StatusCode::ProxyAuthenticationRequired | StatusCode::Unauthorized
                    ) && !self.inner.cancel_requested.load(Ordering::SeqCst)
                    {
                        if !auth.should_answer(&resp) {
                            final_response = Some(resp.clone());
                            debug!(id = %self.id(), ?status, "received auth response after auth sent");
                            self.inner.transition(DialogState::Terminated(
                                self.id(),
                                TerminatedReason::ProxyAuthRequired,
                            ));
                            break;
                        }
                        if let Some(credential) = &self.inner.credential {
                            let challenge = resp.clone();
                            *tx = handle_client_authenticate(
                                self.inner.increment_local_seq(),
                                tx,
                                resp,
                                credential,
                            )
                            .await?;
                            if initial_control
                                .as_ref()
                                .is_some_and(|control| !control.begin_attempt())
                            {
                                final_response = Some(challenge);
                                self.inner.transition(DialogState::Terminated(
                                    self.id(),
                                    TerminatedReason::UacCancel,
                                ));
                                break;
                            }
                            self.inner.reserve_invite(tx)?;
                            self.inner.bind_persistence(tx);
                            tx.track_receipt_times(
                                self.inner.lifecycle.lock().sender.is_some()
                                    || self.inner.persistence.lock().is_some(),
                            );
                            tx.send().await?;
                            self.inner.set_server_connection(tx.connection.clone());
                            self.inner.update_remote_tag("").ok();
                            {
                                let mut req = self.inner.initial_request.lock();
                                *req = tx.original.clone();
                            }
                            continue;
                        } else {
                            // No credential to retry with: the challenge is the
                            // final response (RFC 3261 §22.2, §22.3).
                            final_response = Some(resp);
                            debug!(id = %self.id(), ?status, "received auth challenge without credential");
                            self.inner.transition(DialogState::Terminated(
                                self.id(),
                                TerminatedReason::ProxyAuthRequired,
                            ));
                            break;
                        }
                    }
                    let timer = self.inner.session_timer.lock().clone();
                    if let Some(timer) = timer {
                        if !self.inner.cancel_requested.load(Ordering::SeqCst) {
                            if let Some(next) =
                                timer.retry(&self.inner, tx, &resp, &mut timer_retries)?
                            {
                                *tx = next;
                                if initial_control
                                    .as_ref()
                                    .is_some_and(|control| !control.begin_attempt())
                                {
                                    final_response = Some(resp);
                                    self.inner.transition(DialogState::Terminated(
                                        self.id(),
                                        TerminatedReason::UacCancel,
                                    ));
                                    break;
                                }
                                self.inner.reserve_invite(tx)?;
                                self.inner.update_remote_tag("")?;
                                *self.inner.initial_request.lock() = tx.original.clone();
                                auth = AuthRetry::new();
                                tx.send().await?;
                                self.inner.set_server_connection(tx.connection.clone());
                                continue;
                            }
                        }
                    }
                    final_response = Some(resp.clone());
                    if let Some(tag) = resp.to_header()?.tag()? {
                        self.inner.update_remote_tag(tag.value())?
                    }

                    if let Ok(id) = DialogId::try_from((&resp, TransactionRole::Client)) {
                        dialog_id = id;
                    }
                    match resp.status_code {
                        StatusCode::Ringing | StatusCode::SessionProgress
                            if resp
                                .to_header()
                                .ok()
                                .and_then(|h| h.tag().ok().flatten())
                                .is_some() =>
                        {
                            self.inner.update_route_set_from_response(&resp);
                        }
                        _ if resp.status_code.kind() == StatusCodeKind::Successful => {
                            // Already established before the ACK. Never confirm
                            // again: an incoming BYE may have terminated it.
                            if cancelled && self.inner.initial_operation.lock().upgrade().is_none()
                            {
                                self.bye_2xx_of_cancelled(resp);
                            }
                        }
                        _ => {
                            self.inner.transition(DialogState::Terminated(
                                self.id(),
                                TerminatedReason::UasOther(resp.status_code.clone()),
                            ));
                        }
                    }
                    break;
                }
            }
        }
        Ok((dialog_id, final_response))
    }

    fn establish_from_2xx(
        &self,
        resp: &Response,
        received_at: super::lifecycle::LifecycleTime,
        register: &mut impl FnMut(&DialogId),
    ) -> Result<bool> {
        if let Some(tag) = resp.to_header()?.tag()? {
            self.inner.update_remote_tag(tag.value())?;
        }
        self.update_remote_target_from_2xx(resp)?;
        let id = self.id();
        let confirmed = DialogState::Confirmed(id.clone(), resp.clone());
        Ok(self
            .inner
            .confirm_unless_cancelled(confirmed, received_at, || register(&id)))
    }

    /// Take the route set, Contact and remote target from a 2xx to the INVITE.
    fn update_remote_target_from_2xx(&self, resp: &Response) -> Result<()> {
        self.inner.update_route_set_from_response(resp);
        let contact = resp.contact_header()?;

        let contact_uri = resp
            .typed_contact_headers()?
            .first()
            .map(|c| c.uri.clone())
            .ok_or_else(|| crate::Error::Error("missing Contact header".to_string()))?;
        self.inner
            .set_remote_target(contact_uri, Some(contact.clone()));
        Ok(())
    }

    /// End the session a 2xx established after we cancelled the INVITE.
    ///
    /// A CANCEL that crosses a 2xx has no effect on the INVITE (RFC 3261
    /// §9.1, §15), so the UAC has to send a BYE once the 2xx is ACKed. The
    /// dialog was already abandoned, so no `Confirmed` state is reported.
    pub(super) async fn bye_2xx_after_cancel(&self, resp: &Response) -> Result<()> {
        if let Some(tag) = resp.to_header()?.tag()? {
            self.inner.update_remote_tag(tag.value())?;
        }
        self.update_remote_target_from_2xx(resp)?;
        let control = self.inner.initial_operation.lock().upgrade();
        if let Some(control) = control {
            let operation = self
                .admit_operation(Method::Bye, control.termination_headers(), None, None, true)
                .map_err(|e| crate::Error::Error(e.to_string()))?;
            let result = operation.wait().await;
            let signaling = result
                .signaling_result()
                .map_err(|e| crate::Error::Error(e.to_string()));
            *control.cleanup.lock() = Some(result);
            return signaling;
        }
        let request = self
            .inner
            .make_request(Method::Bye, None, None, None, None, None)?;
        self.inner.do_request(request).await?;
        Ok(())
    }

    /// End a forked dialog a later 2xx established (RFC 3261 §13.2.2.4).
    ///
    /// Called from the Accepted-window drainer for every message the client
    /// INVITE transaction delivers after the dialog confirmed. A 2xx whose To
    /// tag differs from the confirmed dialog's is a forked branch: the
    /// transaction has already ACKed it, and the UAC — keeping a single
    /// session — terminates it with a BYE. Everything else (retransmitted
    /// 2xx with the same tag, non-2xx) is ignored. `seen` holds the forked
    /// tags already BYE'd, so a retransmitted forked 2xx sends one BYE only.
    pub(super) async fn end_forked_branch(
        &self,
        msg: &SipMessage,
        confirmed_remote_tag: &str,
        seen: &mut Vec<String>,
    ) {
        let SipMessage::Response(resp) = msg else {
            return;
        };
        if resp.status_code.kind() != StatusCodeKind::Successful {
            return;
        }
        // Same tag: a retransmission of the confirmed 2xx, already re-ACKed.
        let tag = match resp
            .to_header()
            .ok()
            .and_then(|to| to.tag().ok().flatten())
            .map(|tag| tag.value().to_string())
        {
            Some(tag) => tag,
            None => return,
        };
        if tag == confirmed_remote_tag {
            return;
        }
        if seen.iter().any(|seen| seen == &tag) {
            return;
        }
        seen.push(tag.clone());
        let id = self.id();
        info!(
            id = %id,
            tag = %tag,
            "forked 2xx acknowledged; ending the extra branch with a BYE (RFC 3261 §13.2.2.4)"
        );
        if let Err(e) = self.inner.bye_forked_branch(resp).await {
            warn!(id = %id, tag = %tag, error = %e, "failed to BYE the forked branch");
        }
    }

    // ── Shared request semantics ──────────────────────────────────────────

    /// Convert this INVITE dialog to a subscription dialog (role-aware).
    pub fn as_subscription(&self) -> super::dialog::Dialog {
        match self.role() {
            TransactionRole::Server => Dialog::ServerSubscription(ServerSubscriptionDialog {
                inner: self.inner.clone(),
            }),
            TransactionRole::Client => Dialog::ClientSubscription(ClientSubscriptionDialog {
                inner: self.inner.clone(),
            }),
        }
    }

    // ── Incoming request handling ─────────────────────────────────────────

    pub(crate) async fn handle_admitted(
        &mut self,
        tx: &mut Transaction,
        sequence_admitted: bool,
    ) -> Result<()> {
        let result = self.handle_transaction(tx, sequence_admitted).await;
        tx.finish_ack_dispatch();
        result
    }

    async fn handle_transaction(
        &mut self,
        tx: &mut Transaction,
        sequence_admitted: bool,
    ) -> Result<()> {
        self.inner.bind_persistence(tx);
        if self.inner.is_terminated() && tx.original.method != Method::PRack {
            if tx.original.method != Method::Ack {
                tx.reply(StatusCode::CallTransactionDoesNotExist).await?;
            }
            return Ok(());
        }
        trace!(
            id = %self.id(),
            method = %tx.original.method,
            state = %self.inner.state.lock(),
            "handle request"
        );

        // RFC 7989 §8: a mid-dialog request may carry a new peer UUID;
        // responses to it must mirror the new value. CANCEL is exempt —
        // its Session-ID is always identical to the original INVITE's and
        // MUST NOT update the stored peer UUID.
        if tx.original.method != Method::Cancel {
            let peer_uuid = tx.original.session_id_header().and_then(|s| s.local_uuid());
            self.inner.observe_peer_session_uuid(peer_uuid);
        }

        self.inner.attach_reliable_server(tx)?;
        if self.inner.reject_required_100rel(tx).await? {
            return Ok(());
        }

        let cseq = tx.original.cseq_header()?.seq()?;
        // ACK and CANCEL reuse their INVITE's CSeq and are matched by the
        // transaction layer. Neither participates in the dialog sequence.
        // Re-evaluate the comparison after every competing update: a failed
        // one-shot CAS must not admit a stale request or lose a newer CSeq.
        let sequence =
            if sequence_admitted || matches!(tx.original.method, Method::Ack | Method::Cancel) {
                Ok(self.inner.remote_seq.load(Ordering::Relaxed))
            } else {
                self.inner.admit_remote_seq(cseq, &tx.original.method)
            };
        if let Err(remote_seq) = sequence {
            // The receiver is the UAS of this request, regardless of who
            // originated the dialog (RFC 3261 section 12.2.2).
            debug!(
                id = %self.id(),
                method = %tx.original.method,
                remote_seq = %remote_seq,
                cseq = %cseq,
                "received old request"
            );
            tx.reply(StatusCode::ServerInternalError).await?;
            return Ok(());
        }

        if self.inner.handle_session_timer(tx).await? {
            return Ok(());
        }

        // A locally originated dialog can receive PRACK for its re-INVITE.
        if tx.original.method == Method::PRack {
            return self.handle_prack(tx).await;
        }
        // OPTIONS is valid in an early dialog and while a UAS awaits ACK too.
        // Sequence admission above is shared with confirmed requests; the
        // application supplies response policy, never its own CSeq gate.
        if tx.original.method == Method::Options {
            return self.handle_options(tx).await;
        }
        if tx.original.method == Method::Info && self.inner.waiting_ack() {
            let (handle, rx) = TransactionHandle::new();
            self.inner
                .transition(DialogState::Info(self.id(), tx.original.clone(), handle));
            return self.inner.process_transaction_handle(tx, rx).await;
        }
        if self.inner.is_confirmed() {
            // Validate before the application can change its media state or
            // accept the request. Receiving a Contact alone does not commit it.
            if DialogInner::is_target_refresh(&tx.original.method)
                && DialogInner::parse_target_refresh_contact(&tx.original).is_err()
            {
                tx.reply(StatusCode::BadRequest).await?;
                return Ok(());
            }
            if !self.admit_incoming_invite(tx).await? {
                return Ok(());
            }
            match tx.original.method {
                Method::Cancel => {
                    // UAS replies 200 to a CANCEL in confirmed state; UAC
                    // treats it as invalid.
                    if self.role() == TransactionRole::Server {
                        debug!(
                            id = %self.id(),
                            method = %tx.original.method,
                            uri = %tx.original.uri,
                            "received cancel in confirmed state"
                        );
                        tx.reply(StatusCode::OK).await?;
                        return Ok(());
                    }
                }
                Method::Ack => {
                    if self.role() == TransactionRole::Server {
                        debug!(
                            id = %self.id(),
                            method = %tx.original.method,
                            uri = %tx.original.uri,
                            "received ack in confirmed state"
                        );
                        return Err(crate::Error::DialogError(
                            "invalid request in confirmed state".to_string(),
                            self.id(),
                            StatusCode::MethodNotAllowed,
                        ));
                    }
                }
                Method::Invite => return self.handle_reinvite(tx).await,
                Method::Bye => return self.handle_bye(tx).await,
                Method::PRack => return self.handle_prack(tx).await,
                Method::Info => return self.handle_info(tx).await,
                Method::Options => return self.handle_options(tx).await,
                Method::Update => return self.handle_update(tx).await,
                Method::Refer => return self.handle_refer(tx).await,
                Method::Message => return self.handle_message(tx).await,
                Method::Notify => return self.handle_notify(tx).await,
                _ => {
                    debug!(id = %self.id(), method = ?tx.original.method, "invalid request method");
                    tx.reply(StatusCode::MethodNotAllowed).await?;
                    return Err(crate::Error::DialogError(
                        "invalid request".to_string(),
                        self.id(),
                        StatusCode::MethodNotAllowed,
                    ));
                }
            }
        }

        if !self.admit_incoming_invite(tx).await? {
            return Ok(());
        }
        // Not confirmed — only the UAS side drives the INVITE transaction here.
        if self.role() == TransactionRole::Server {
            match tx.original.method {
                Method::Invite => return self.handle_invite(tx).await,
                Method::PRack => return self.handle_prack(tx).await,
                Method::Ack => {
                    self.inner.tu_sender.send(TransactionEvent::Received(
                        tx.original.clone().into(),
                        tx.connection.clone(),
                    ))?;
                    return Ok(());
                }
                // Accept BYE even in WaitAck state — remote may tear down call
                // before ACK arrives (common with SIP proxies)
                Method::Bye => return self.handle_bye(tx).await,
                _ => {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Apply RFC 3261 §14.2 before dispatching an INVITE to the application.
    async fn admit_incoming_invite(&self, tx: &mut Transaction) -> Result<bool> {
        if tx.original.method != Method::Invite {
            return Ok(true);
        }
        let Err(pending_role) = tx.reserve_invite(&self.inner.invite_usage) else {
            return Ok(true);
        };
        let (status, headers) = match pending_role {
            TransactionRole::Client => (StatusCode::RequestPending, None),
            TransactionRole::Server => {
                let mut random = [0u8; 4];
                crate::transaction::fill_random_bytes(&mut random);
                let delay = u32::from_ne_bytes(random) % 11;
                (
                    StatusCode::ServerInternalError,
                    Some(vec![Header::RetryAfter(delay.to_string().into())]),
                )
            }
        };
        let response = self
            .inner
            .make_response(&tx.original, status, headers, None);
        tx.respond(response).await?;
        Ok(false)
    }

    async fn handle_bye(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received bye");
        let reason = match self.role() {
            TransactionRole::Server => TerminatedReason::UacBye,
            TransactionRole::Client => TerminatedReason::UasBye,
        };
        self.inner
            .terminate_remote_bye(
                reason,
                tx.original_received_at
                    .unwrap_or_else(super::lifecycle::LifecycleTime::now),
                &tx.original,
            )
            .await?;
        let incoming = self.inner.incoming_invite.lock().upgrade();
        let final_response = incoming
            .map(|incoming| incoming.finish_early_bye(&self.inner))
            .transpose();
        tx.reply(StatusCode::OK).await?;
        if let Some(Some(receipt)) =
            final_response.map_err(|e| crate::Error::Error(e.to_string()))?
        {
            receipt
                .wait_sent()
                .await
                .map_err(|e| crate::Error::Error(e.to_string()))?;
        }
        Ok(())
    }

    async fn handle_info(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received info");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Info(self.id(), tx.original.clone(), handle));
        let result = self.inner.process_transaction_handle(tx, rx).await;
        self.return_to_confirmed(tx);
        result
    }

    /// Restore confirmation after the mid-dialog handlers that use this helper.
    /// REFER is delivered as an event without leaving Confirmed and does not
    /// need another confirmation event after its response.
    fn return_to_confirmed(&self, tx: &Transaction) {
        self.inner.transition(DialogState::Confirmed(
            self.id(),
            tx.last_response.clone().unwrap_or_default(),
        ));
    }

    async fn handle_prack(&mut self, tx: &mut Transaction) -> Result<()> {
        self.inner.receive_prack(tx).await
    }

    async fn handle_options(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received options");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Options(self.id(), tx.original.clone(), handle));

        self.inner.process_transaction_handle(tx, rx).await
    }

    async fn handle_update(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received update");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Updated(self.id(), tx.original.clone(), handle));

        let result = self.inner.process_transaction_handle(tx, rx).await;
        self.return_to_confirmed(tx);
        result
    }

    async fn handle_refer(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received refer");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Refer(self.id(), tx.original.clone(), handle));

        // RFC 3515: the REFER was answered (usually 202) and the dialog must
        // go back to Confirmed — the implicit subscription's NOTIFYs are
        // in-dialog requests that need the confirmed dialog.
        let result = self.inner.process_transaction_handle(tx, rx).await;
        self.return_to_confirmed(tx);
        result
    }

    async fn handle_message(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received message");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Message(self.id(), tx.original.clone(), handle));

        let result = self.inner.process_transaction_handle(tx, rx).await;
        self.return_to_confirmed(tx);
        result
    }

    async fn handle_notify(&mut self, tx: &mut Transaction) -> Result<()> {
        debug!(id = %self.id(), uri = %tx.original.uri, "received notify");
        let (handle, rx) = TransactionHandle::new();
        self.inner
            .transition(DialogState::Notify(self.id(), tx.original.clone(), handle));

        let result = self.inner.process_transaction_handle(tx, rx).await;
        self.return_to_confirmed(tx);
        result
    }

    async fn handle_reinvite(&mut self, tx: &mut Transaction) -> Result<()> {
        self.inner.begin_negotiation();
        let result = self.inner.process_reinvite(tx, true).await;
        self.inner.finish_negotiation();
        self.inner
            .checkpoint(
                super::persistence::DialogCheckpointReason::SignalingCompleted,
                None,
            )
            .await?;
        result
    }

    async fn handle_invite(&mut self, tx: &mut Transaction) -> Result<()> {
        *tx.initial_dialog.lock() = Some(Arc::downgrade(&self.inner));
        let handle_loop = async {
            if matches!(tx.original.method, Method::Invite)
                && self.inner.transition_if_calling()
                && tx.last_response.is_none()
            {
                tx.send_trying().await.ok();
            }

            while let Some(msg) = tx.receive().await {
                match msg {
                    SipMessage::Request(req) => match req.method {
                        Method::Ack => {
                            if self.inner.is_terminated() {
                                break;
                            }
                            debug!(id = %self.id(), "received ack {}", req.uri);
                            self.inner.remote_ack.lock().replace(req);
                            self.inner.transition(DialogState::Confirmed(
                                self.id(),
                                tx.last_response.clone().unwrap_or_default(),
                            ));
                            break;
                        }
                        Method::Cancel => {
                            debug!(id = %self.id(), "received cancel {}", req.uri);
                            tx.reply(StatusCode::RequestTerminated).await?;
                            self.inner.transition(DialogState::Terminated(
                                self.id(),
                                TerminatedReason::UacCancel,
                            ));
                            break;
                        }
                        _ => {}
                    },
                    SipMessage::Response(_) => {}
                }
            }
            tx.check_persistence()?;
            let answered_2xx = self.inner.waiting_ack();
            self.inner.end_session_without_ack(tx, answered_2xx).await;
            self.inner
                .checkpoint(
                    super::persistence::DialogCheckpointReason::SignalingCompleted,
                    None,
                )
                .await?;
            Ok::<(), crate::Error>(())
        };
        match handle_loop.await {
            Ok(_) => {
                trace!(id = %self.id(), "process done");
                Ok(())
            }
            Err(e) => {
                warn!(id = %self.id(), "handle_invite error: {:?}", e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glare_backoff_uses_call_id_owner_window_and_ten_ms_units() {
        for role in [TransactionRole::Client, TransactionRole::Server] {
            for _ in 0..256 {
                let delay = glare_backoff_ms(role);
                match role {
                    TransactionRole::Client => assert!((2100..=4000).contains(&delay)),
                    TransactionRole::Server => assert!((0..=2000).contains(&delay)),
                }
                assert_eq!(delay % 10, 0, "RFC 3261 section 14.1 uses 10 ms units");
            }
        }
    }
}

impl InviteDialog {
    /// Protocol cleanup inside an existing driver; never starts a nested operation.
    pub(super) async fn send_bye_headers(&self, headers: Option<Vec<Header>>) -> Result<()> {
        let confirmed_or_waiting_ack = self.inner.is_confirmed()
            || (self.role() == TransactionRole::Server && self.inner.waiting_ack());
        if !confirmed_or_waiting_ack {
            if !self.inner.is_terminated() {
                warn!(
                    dialog_id = %self.id(),
                    state = %self.state(),
                    "bye skipped: dialog not confirmed or waiting ack"
                );
                return Err(crate::Error::Error(format!(
                    "dialog {} cannot send BYE in state {}",
                    self.id(),
                    self.state()
                )));
            }
            return Ok(());
        }

        let request = self
            .inner
            .make_request(Method::Bye, None, None, None, headers, None)?;

        let reason = match self.role() {
            TransactionRole::Server => TerminatedReason::UasBye,
            TransactionRole::Client => TerminatedReason::UacBye,
        };
        self.inner.send_bye(request, reason).await
    }
}

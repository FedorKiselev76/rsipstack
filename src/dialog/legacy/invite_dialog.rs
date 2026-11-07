use crate::dialog::{
    dialog::{DialogState, TerminatedReason},
    invite_dialog::{glare_backoff_ms, InviteDialog},
    DialogId,
};
use crate::{
    prelude::*,
    sip::{Header, Method, Response, StatusCode},
    transaction::{
        key::TransactionRole,
        transaction::{Transaction, TransactionEvent},
    },
    Result,
};
use core::sync::atomic::Ordering;
use tracing::{debug, warn};

impl InviteDialog {
    /// Send `180 Ringing` (or `183 Session Progress` when a body is given).
    /// No-op for UAC dialogs.
    #[deprecated(note = "Use start_response to retain stack ownership and observe completion")]
    pub fn ringing(&self, headers: Option<Vec<Header>>, body: Option<Vec<u8>>) -> Result<()> {
        if self.role() != TransactionRole::Server {
            return Ok(());
        }
        if !self.inner.can_cancel() {
            return Ok(());
        }
        debug!(id = %self.id(), "sending ringing response");
        let resp = self.inner.make_response(
            &self.initial_request(),
            if body.is_some() {
                StatusCode::SessionProgress
            } else {
                StatusCode::Ringing
            },
            headers,
            body,
        );
        self.inner
            .tu_sender
            .send(TransactionEvent::Respond(resp.clone()))?;
        if self.inner.reliable_config.lock().is_none() || !self.inner.supports_100rel {
            self.inner.transition(DialogState::Early(self.id(), resp));
        }
        Ok(())
    }

    /// Accept the incoming INVITE with a `200 OK`.
    /// No-op for UAC dialogs.
    #[deprecated(note = "Use start_response to retain stack ownership and observe completion")]
    pub fn accept(&self, headers: Option<Vec<Header>>, body: Option<Vec<u8>>) -> Result<()> {
        if self.role() != TransactionRole::Server {
            return Ok(());
        }
        let resp = self
            .inner
            .make_response(&self.initial_request(), StatusCode::OK, headers, body);
        self.inner
            .tu_sender
            .send(TransactionEvent::Respond(resp.clone()))?;
        if self.inner.reliable_config.lock().is_none() || !self.inner.supports_100rel {
            self.inner.transition(DialogState::WaitAck(self.id(), resp));
        }
        Ok(())
    }

    /// Accept the incoming INVITE with a NAT-aware public Contact header.
    /// No-op for UAC dialogs.
    #[deprecated(note = "Use start_response to retain stack ownership and observe completion")]
    pub fn accept_with_public_contact(
        &self,
        username: &str,
        public_address: Option<crate::sip::HostWithPort>,
        local_address: &crate::transport::SipAddr,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<()> {
        use crate::dialog::registration::Registration;

        let contact_header =
            Registration::create_nat_aware_contact(username, public_address, local_address);

        let mut final_headers = headers.unwrap_or_default();
        final_headers.push(contact_header.into());

        self.accept(Some(final_headers), body)
    }

    /// Reject the incoming INVITE (default `603 Decline`).
    /// No-op for UAC dialogs.
    #[deprecated(note = "Use start_response to retain stack ownership and observe completion")]
    pub fn reject(&self, code: Option<StatusCode>, reason: Option<String>) -> Result<()> {
        if self.role() != TransactionRole::Server {
            return Ok(());
        }
        if self.inner.is_terminated() || self.inner.is_confirmed() {
            return Ok(());
        }
        {
            let state = self.inner.state.lock();
            if state.is_terminated() || state.is_confirmed() {
                return Ok(());
            }
            self.inner.record_local_termination();
        }
        debug!(id = %self.id(), ?code, ?reason, "rejecting dialog");
        let headers = reason.map(|reason| vec![Header::Reason(reason.into())]);
        let resp = self.inner.make_response(
            &self.initial_request(),
            code.unwrap_or(StatusCode::Decline),
            headers,
            None,
        );
        self.inner
            .tu_sender
            .send(TransactionEvent::Respond(resp))
            .ok();
        self.inner.transition(DialogState::Terminated(
            self.id(),
            TerminatedReason::UasDecline,
        ));
        Ok(())
    }

    /// Send a BYE request to terminate the dialog.
    /// No-op for UAS dialogs (which terminate via `hangup_with_headers`).
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn hangup(&self) -> Result<()> {
        self.hangup_with_headers(None).await
    }

    /// Terminate the dialog: send BYE when confirmed, CANCEL when still early.
    /// Role-agnostic: an unanswered UAS dialog is left alone (it ends with
    /// `reject()`), and `bye_with_headers()` handles the confirmed UAS/UAC
    /// case uniformly.
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn hangup_with_headers(&self, headers: Option<Vec<Header>>) -> Result<()> {
        if self.inner.can_cancel() {
            if self.role() != TransactionRole::Client {
                return Ok(());
            }
            self.cancel().await
        } else {
            self.bye_with_headers(headers).await
        }
    }

    /// Hang up and attach a SIP `Reason` header to the BYE.
    /// No-op for UAS dialogs.
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn hangup_with_reason(&self, reason: String) -> Result<()> {
        self.hangup_with_headers(Some(vec![Header::Reason(reason.into())]))
            .await
    }

    /// Send a CANCEL request to abort an unanswered INVITE.
    ///
    /// * `Calling` (no response yet): returns `Ok(())` at once. The CANCEL
    ///   is sent when the first provisional response arrives (RFC 3261
    ///   §9.1); `process_invite` (`do_invite`, `do_invite_async`) sends it.
    /// * `Trying` / `Early`: sends the CANCEL and returns its outcome.
    ///
    /// In both cases a 2xx to the INVITE is ACKed and then ended with a BYE
    /// (with `auto_ack_2xx` off the ACK stays the caller's), the dialog
    /// reports `Terminated(UacCancel)`, never `Confirmed`, and is not
    /// registered in the dialog layer. A challenge (401/407) is final.
    /// Once a final response arrived there is nothing to cancel: `Ok(())`.
    /// A UAS cannot send CANCEL: an error; end the INVITE with `reject()`.
    #[deprecated(
        note = "Use start_cancel or start_termination to retain stack ownership and observe completion"
    )]
    pub async fn cancel(&self) -> Result<()> {
        let control = self.inner.initial_operation.lock().upgrade();
        if let Some(control) = control {
            return control
                .cancel()
                .wait()
                .await
                .signaling_result()
                .map_err(|error| crate::Error::Error(error.to_string()));
        }
        if self.role() != TransactionRole::Client {
            return Err(crate::Error::DialogError(
                "cancel is a UAC operation; use reject()".to_string(),
                self.id(),
                StatusCode::ServerInternalError,
            ));
        }
        let deferred = {
            // Decided under the state lock, so `process_invite` either sees
            // the deferred CANCEL after leaving Calling or we see it left.
            let state = self.inner.state.lock();
            match *state {
                DialogState::Calling(_) => {
                    self.inner.record_local_termination();
                    self.inner.cancel_requested.store(true, Ordering::SeqCst);
                    self.inner.cancel_deferred.store(true, Ordering::SeqCst);
                    true
                }
                DialogState::Trying(_) | DialogState::Early(_, _) => {
                    self.inner.record_local_termination();
                    self.inner.cancel_requested.store(true, Ordering::SeqCst);
                    false
                }
                _ => return Ok(()),
            }
        };
        self.inner
            .checkpoint(
                crate::dialog::persistence::DialogCheckpointReason::LocalTerminationRequested,
                None,
            )
            .await?;
        if deferred {
            Ok(())
        } else {
            self.send_cancel().await
        }
    }

    /// Send an OPTIONS request within the dialog.
    /// No-op for UAS dialogs.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn options(&self, headers: Option<Vec<Header>>) -> Result<Option<Response>> {
        if self.role() != TransactionRole::Client {
            return Ok(None);
        }
        self.request(Method::Options, headers, None).await
    }

    /// Drive the outbound INVITE transaction until a final response arrives.
    /// Only meaningful for UAC dialogs.
    /// `Confirmed` is notified before sending the initial ACK, allowing an
    /// incoming BYE immediately. This method returns after the ACK attempt;
    /// local `bye()`/`hangup()` calls wait for that attempt to complete.
    #[deprecated(
        note = "Use DialogLayer::start_invite to retain stack ownership and observe completion"
    )]
    pub async fn process_invite(
        &self,
        tx: &mut Transaction,
    ) -> Result<(DialogId, Option<Response>)> {
        self.process_invite_with_registration(tx, |_| {}).await
    }

    /// Send a BYE request to terminate the dialog.
    /// Convenience alias for [`bye_with_headers`](Self::bye_with_headers).
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn bye(&self) -> Result<()> {
        self.bye_with_headers(None).await
    }

    /// Send a BYE request with custom headers (role-aware termination reason).
    ///
    /// BYE can only be sent from `Confirmed` (or `WaitAck` for a server dialog).
    /// A UAC waits for its initial ACK attempt before sending the BYE.
    /// Calling BYE in any other non-terminated state returns an error; terminated
    /// dialogs remain a silent no-op.
    ///
    /// # Returns
    /// * `Ok(())` - BYE was sent successfully or dialog is already terminated.
    /// * `Err(Error)` - Failed to build/send BYE request, or dialog is in a state where BYE does not apply.
    ///
    /// Once the BYE is handed to its transaction the dialog is `Terminated`,
    /// even when an error is returned (RFC 3261 §15.1.1).
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn bye_with_headers(&self, headers: Option<Vec<Header>>) -> Result<()> {
        self.send_bye_headers(headers).await
    }

    /// Send a BYE request with a SIP `Reason` header.
    #[deprecated(note = "Use start_termination to retain stack ownership and observe completion")]
    pub async fn bye_with_reason(&self, reason: String) -> Result<()> {
        self.bye_with_headers(Some(vec![Header::Reason(reason.into())]))
            .await
    }

    /// Send a re-INVITE request to modify the session.
    /// Returns `DialogError` with `RequestPending` while another INVITE
    /// transaction prevents a new local INVITE (RFC 3261 §14.1).
    #[deprecated(note = "Use start_reinvite to retain stack ownership and observe completion")]
    pub async fn reinvite(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        if !self.inner.is_confirmed() {
            return Ok(None);
        }
        debug!(
            id = %self.id(),
            body = ?body.as_deref().map(String::from_utf8_lossy),
            "sending re-invite request"
        );
        let request = self
            .inner
            .make_request(Method::Invite, None, None, None, headers, body)?;
        self.inner.do_request(request).await
    }

    /// re-INVITE with glare handling (RFC 3261 §14.1): on 491 retry after a
    /// jittered backoff, up to `max_attempts` total. The original caller
    /// (Call-ID owner) waits 2.1–4.0 s; the original callee waits 0–2.0 s.
    #[deprecated(note = "Use start_reinvite to retain stack ownership and observe completion")]
    pub async fn reinvite_with_glare_retry(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        max_attempts: usize,
    ) -> Result<Option<Response>> {
        for attempt in 0..max_attempts {
            let resp = self.reinvite(headers.clone(), body.clone()).await?;
            let glare = matches!(
                resp.as_ref(),
                Some(r) if r.status_code == StatusCode::RequestPending
            );
            if !glare {
                return Ok(resp);
            }
            if attempt + 1 == max_attempts {
                return Err(crate::Error::Error(alloc::format!(
                    "re-INVITE glare: exhausted {max_attempts} retries (491)"
                )));
            }
            let backoff_ms = glare_backoff_ms(self.role());
            warn!(
                id = %self.id(),
                attempt,
                backoff_ms,
                "re-INVITE glare (491), retrying"
            );
            crate::platform::sleep(core::time::Duration::from_millis(backoff_ms)).await;
        }
        Err(crate::Error::Error(alloc::string::String::from(
            "re-INVITE glare: no attempts made",
        )))
    }

    /// Send an UPDATE request to modify session parameters.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn update(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        if !self.inner.is_confirmed() {
            return Ok(None);
        }
        debug!(
            id = %self.id(),
            body = ?body.as_deref().map(String::from_utf8_lossy),
            "sending update request"
        );
        let request = self
            .inner
            .make_request(Method::Update, None, None, None, headers, body)?;
        self.inner.do_request(request.clone()).await
    }

    /// Send an INFO request for mid-dialog information.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn info(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        if !self.inner.is_confirmed() {
            return Ok(None);
        }
        debug!(
            id = %self.id(),
            body = ?body.as_deref().map(String::from_utf8_lossy),
            "sending info request"
        );
        let request = self
            .inner
            .make_request(Method::Info, None, None, None, headers, body)?;
        self.inner.do_request(request.clone()).await
    }

    /// Send a generic in-dialog request.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn request(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        if !self.inner.is_confirmed() {
            return Ok(None);
        }
        debug!(id = %self.id(), %method, "sending request");
        let request = self
            .inner
            .make_request(method, None, None, None, headers, body)?;
        self.inner.do_request(request).await
    }

    /// Send a NOTIFY request.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn notify(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        self.request(Method::Notify, headers, body).await
    }

    /// Send a REFER request to transfer the call.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn refer(
        &self,
        refer_to: impl Into<crate::sip::ReferTo>,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        let mut headers = headers.unwrap_or_default();
        headers.push(Header::ReferTo(refer_to.into()));
        self.request(Method::Refer, Some(headers), body).await
    }

    /// Send a REFER progress notification (RFC 3515).
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn notify_refer(
        &self,
        status: StatusCode,
        sub_state: &str,
    ) -> Result<Option<Response>> {
        let headers = vec![
            Header::Event("refer".into()),
            Header::SubscriptionState(sub_state.into()),
            Header::ContentType("message/sipfrag".into()),
        ];

        let body = format!("SIP/2.0 {} {:?}", u16::from(status.clone()), status).into_bytes();

        self.notify(Some(headers), Some(body)).await
    }

    /// Send a MESSAGE request within the dialog.
    #[deprecated(note = "Use start_request to retain stack ownership and observe completion")]
    pub async fn message(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Response>> {
        self.request(Method::Message, headers, body).await
    }

    /// Handle an incoming transaction routed to this dialog.
    #[deprecated(
        note = "Use start_incoming_request to retain stack ownership and observe completion"
    )]
    pub async fn handle(&mut self, tx: &mut Transaction) -> Result<()> {
        self.handle_admitted(tx, false).await
    }
}
impl From<crate::dialog::server_dialog::ServerInviteDialog> for InviteDialog {
    fn from(d: crate::dialog::server_dialog::ServerInviteDialog) -> Self {
        Self { inner: d.inner }
    }
}

impl From<crate::dialog::client_dialog::ClientInviteDialog> for InviteDialog {
    fn from(d: crate::dialog::client_dialog::ClientInviteDialog) -> Self {
        Self { inner: d.inner }
    }
}

impl TryFrom<InviteDialog> for crate::dialog::server_dialog::ServerInviteDialog {
    type Error = crate::Error;

    fn try_from(d: InviteDialog) -> Result<Self> {
        if d.role() != TransactionRole::Server {
            return Err(crate::Error::DialogError(
                "InviteDialog is not a Server dialog".to_string(),
                d.id(),
                StatusCode::BadRequest,
            ));
        }
        Ok(Self { inner: d.inner })
    }
}

impl TryFrom<InviteDialog> for crate::dialog::client_dialog::ClientInviteDialog {
    type Error = crate::Error;

    fn try_from(d: InviteDialog) -> Result<Self> {
        if d.role() != TransactionRole::Client {
            return Err(crate::Error::DialogError(
                "InviteDialog is not a Client dialog".to_string(),
                d.id(),
                StatusCode::BadRequest,
            ));
        }
        Ok(Self { inner: d.inner })
    }
}

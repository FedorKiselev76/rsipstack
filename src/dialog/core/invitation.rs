use super::{
    authenticate::Credential,
    dialog::{DialogInner, DialogStateSender},
    dialog_layer::DialogLayer,
    invite_dialog::InviteDialog,
};
use crate::prelude::*;
use crate::sip::{
    prelude::{HeadersExt, ToTypedHeader},
    uri::ParamsExt,
    Request, Response, StatusCodeKind,
};
use crate::{
    dialog::{
        dialog::{Dialog, DialogState, TerminatedReason},
        dialog_layer::DialogLayerInnerRef,
        DialogId,
    },
    transaction::{
        key::{TransactionKey, TransactionRole},
        make_tag,
        transaction::Transaction,
    },
    transport::SipAddr,
    Result,
};
use futures::FutureExt;
use tracing::{debug, info, warn};

/// INVITE Request Options
///
/// `InviteOption` contains all the parameters needed to create and send
/// an INVITE request to establish a SIP session. This structure provides
/// a convenient way to specify all the necessary information for initiating
/// a call or session.
///
/// # Fields
///
/// * `caller` - URI of the calling party (From header)
/// * `callee` - URI of the called party (To header and Request-URI)
/// * `content_type` - MIME type of the message body (default: "application/sdp")
/// * `offer` - Optional message body (typically SDP offer)
/// * `contact` - Contact URI for this user agent
/// * `credential` - Optional authentication credentials
/// * `headers` - Optional additional headers to include
///
/// # Examples
///
/// ## Basic Voice Call
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_offer_bytes = vec![];
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     content_type: Some("application/sdp".to_string()),
///     offer: Some(sdp_offer_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     ..Default::default()
/// };
/// # Ok(())
/// # }
/// ```
///
/// ```rust,no_run
/// # use rsipstack::dialog::dialog_layer::DialogLayer;
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let dialog_layer: DialogLayer = todo!();
/// # let invite_option: InviteOption = todo!();
/// let request = dialog_layer.make_invite_request(&invite_option)?;
/// println!("Created INVITE to: {}", request.uri);
/// # Ok(())
/// # }
/// ```
///
/// ## Call with Custom Headers
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_bytes = vec![];
/// # let auth_credential = todo!();
/// let custom_headers = vec![
///     rsipstack::sip::Header::UserAgent("MyApp/1.0".into()),
///     rsipstack::sip::Header::Subject("Important Call".into()),
/// ];
///
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     content_type: Some("application/sdp".to_string()),
///     offer: Some(sdp_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     credential: Some(auth_credential),
///     headers: Some(custom_headers),
///     ..Default::default()
/// };
/// # Ok(())
/// # }
/// ```
///
/// ## Call with Authentication
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # use rsipstack::dialog::authenticate::Credential;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_bytes = vec![];
/// let credential = Credential {
///     username: "alice".to_string(),
///     password: "secret123".to_string(),
///     realm: Some("example.com".to_string()),
///     auth_username: None,
/// };
///
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     offer: Some(sdp_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     credential: Some(credential),
///     ..Default::default()
/// };
/// # Ok(())
/// # }
/// ```
#[derive(Default, Clone)]
pub struct InviteOption {
    pub caller_display_name: Option<String>,
    pub caller_params: Vec<crate::sip::uri::Param>,
    pub caller: crate::sip::Uri,
    pub callee: crate::sip::Uri,
    /// Optional Request-URI distinct from the logical callee (To). Useful for
    /// gateways that require a routing prefix without changing the called party.
    pub request_uri: Option<crate::sip::Uri>,
    /// Optional raw provisional-response observer for owned initial operations.
    /// Receiving a response does not mean its extensions or SDP were accepted.
    pub provisional_responses: Option<crate::platform::mpsc::UnboundedSender<Response>>,
    pub destination: Option<SipAddr>,
    pub content_type: Option<String>,
    pub offer: Option<Vec<u8>>,
    /// Local Contact. On stream transports its host and port are normally
    /// replaced with the outgoing connection's local address (and the scheme
    /// becomes `sips` over TLS). A Contact that already carries a `transport`
    /// parameter is treated as deliberately built by the caller, for example
    /// from the address a registrar reported behind NAT, and is sent unchanged.
    pub contact: crate::sip::Uri,
    pub credential: Option<Credential>,
    /// Extra headers appended to the INVITE. A `Max-Forwards` or
    /// `Content-Type` here replaces the one the stack would add, so it is
    /// never sent twice; a `Content-Type` here wins over `content_type`.
    pub headers: Option<Vec<crate::sip::Header>>,
    /// Legacy client-side advertisement switch. For full UAC/UAS support,
    /// configure `DialogLayer::set_reliable_provisional` with a `PrackHandler`.
    /// That configuration also advertises 100rel; this flag can remain false.
    pub support_prack: bool,
    pub call_id: Option<String>,
    /// RFC 7989: local Session-ID UUID for this call (32 hex chars; dashed
    /// RFC 4122 input is normalized). When `None` no Session-ID header is
    /// generated. Reuse the same value on transferred calls (REFER/Replaces)
    /// to keep the session identifiable across dialogs.
    pub session_id: Option<String>,
    /// RFC 3608: preloaded route set for this out-of-dialog request. Each entry
    /// is emitted as a `Route` header, in order, ahead of the caller-supplied
    /// headers. Typically obtained from
    /// [`Registration::preloaded_route_set`](crate::dialog::registration::Registration::preloaded_route_set)
    /// so the request follows the path an IMS S-CSCF advertised at
    /// registration. Empty by default, leaving existing behaviour unchanged.
    pub route_set: Vec<crate::sip::typed::Route>,
}

#[cfg(not(rsipstack_no_legacy))]
pub use super::legacy::invitation::DialogGuard;

pub type InviteAsyncResult = Result<(DialogId, Option<Response>)>;

impl DialogLayer {
    /// Create an INVITE request from options
    ///
    /// Constructs a properly formatted SIP INVITE request based on the
    /// provided options. This method handles all the required headers
    /// and parameters according to RFC 3261.
    ///
    /// # Parameters
    ///
    /// * `opt` - INVITE options containing all necessary parameters
    ///
    /// # Returns
    ///
    /// * `Ok(Request)` - Properly formatted INVITE request
    /// * `Err(Error)` - Failed to create request
    ///
    /// # Generated Headers
    ///
    /// The method automatically generates:
    /// * Via header with branch parameter
    /// * From header with tag parameter
    /// * To header (without tag for initial request)
    /// * Contact header
    /// * Content-Type header
    /// * CSeq header with incremented sequence number
    /// * Call-ID header
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// let request = dialog_layer.make_invite_request(&invite_option)?;
    /// println!("Created INVITE to: {}", request.uri);
    /// # Ok(())
    /// # }
    /// ```
    pub fn make_invite_request(&self, opt: &InviteOption) -> Result<Request> {
        let last_seq = self.increment_last_seq();
        let to = crate::sip::typed::To {
            display_name: None,
            uri: opt.callee.clone(),
            params: vec![],
        };
        let recipient = opt.request_uri.clone().unwrap_or_else(|| to.uri.clone());

        let from = crate::sip::typed::From {
            display_name: opt.caller_display_name.clone(),
            uri: opt.caller.clone(),
            params: opt.caller_params.clone(),
        }
        .with_tag(make_tag());

        let call_id = opt
            .call_id
            .as_ref()
            .map(|id| crate::sip::headers::CallId::from(id.clone()));

        let target_transport = opt
            .destination
            .as_ref()
            .and_then(|d| d.r#type)
            .or_else(|| opt.callee.transport().cloned())
            .filter(|t| *t != crate::sip::Transport::Udp);

        let transport_addr = target_transport.and_then(|transport| {
            // Listeners only (see `DialogInner::via_addr_for_send_transport`):
            // a connection-cache match advertised the remote peer's address as
            // our own Via on initial INVITEs toward WS/WSS targets.
            let listens = self.endpoint.transport_layer.get_listen_addrs();
            listens
                .iter()
                .find(|a| a.r#type == Some(transport))
                .cloned()
                .or_else(|| {
                    let mut addr = listens.first().cloned()?;
                    addr.r#type = Some(transport);
                    Some(addr)
                })
        });

        let via = self.endpoint.get_via(transport_addr.clone(), None)?;
        let mut request = self.endpoint.make_request(
            crate::sip::Method::Invite,
            recipient,
            via,
            from,
            to,
            last_seq,
            call_id,
        );

        // RFC 3608: preload the Service-Route set learned at registration as
        // Route headers, in order, so this out-of-dialog request traverses the
        // proxies the registrar (e.g. an IMS S-CSCF) requires. Plain push, not
        // unique_push, because a route set legitimately has several Route
        // headers.
        for route in &opt.route_set {
            request.headers.push(route.clone().into());
        }

        // A Contact that names its transport was built deliberately by the
        // caller (e.g. with the public address learned from a REGISTER
        // response behind NAT); keep it rather than rewriting it to the local
        // socket address.
        let caller_built_contact = opt
            .contact
            .params
            .iter()
            .any(|p| matches!(p, crate::sip::Param::Transport(_)));
        let contact = if let Some(addr) = transport_addr.as_ref().filter(|_| !caller_built_contact)
        {
            let mut uri = opt.contact.clone();
            uri.host_with_port = addr.addr.clone();
            if !uri
                .params
                .iter()
                .any(|p| matches!(p, crate::sip::Param::Transport(_)))
            {
                uri.params.push(crate::sip::Param::Transport(
                    addr.r#type.unwrap_or(crate::sip::Transport::Tcp),
                ));
            }
            if addr.r#type == Some(crate::sip::Transport::Tls) {
                uri.scheme = Some(crate::sip::Scheme::Sips);
            }
            crate::sip::typed::Contact {
                display_name: None,
                uri,
                params: vec![],
            }
        } else {
            crate::sip::typed::Contact {
                display_name: None,
                uri: opt.contact.clone(),
                params: vec![],
            }
        };

        request
            .headers
            .unique_push(crate::sip::Header::Contact(contact.into()));

        request.headers.unique_push(crate::sip::Header::ContentType(
            opt.content_type
                .clone()
                .unwrap_or("application/sdp".to_string())
                .into(),
        ));

        if let Some(config) = self.inner.reliable_config.lock().as_ref() {
            super::reliable::add_option(&mut request, "Supported", "100rel");
            if config.require {
                super::reliable::add_option(&mut request, "Require", "100rel");
            }
        }
        if opt.support_prack && !request.header_contains_token("Supported", "100rel") {
            request
                .headers
                .unique_push(crate::sip::Header::Supported("100rel".into()));
        }

        // RFC 7044 §6.1: advertise histinfo so peers include History-Info in
        // responses. Plain push — multiple Supported header lines are legal.
        // The request's own headers and the caller-supplied ones (appended
        // below) are both checked to avoid duplicates.
        if self.endpoint.option.history_info_enabled {
            let already = request
                .headers
                .iter()
                .chain(opt.headers.iter().flatten())
                .any(|h| {
                    matches!(h, crate::sip::Header::Supported(s)
                        if s.value().split(',').any(|t| t.trim().eq_ignore_ascii_case("histinfo")))
                });
            if !already {
                request
                    .headers
                    .push(crate::sip::Header::Supported("histinfo".into()));
            }
        }

        // RFC 7989: only include Session-ID when the application opted in;
        // initial requests carry `<local>;remote=<nil>`.
        if let Some(raw) = opt.session_id.as_deref() {
            let uuid = crate::sip::headers::SessionId::normalize(raw)?;
            request
                .headers
                .unique_push(crate::sip::headers::SessionId::from_local(&uuid)?.into());
        }
        // can't override default headers
        if let Some(headers) = opt.headers.as_ref() {
            for header in headers {
                // only override "max-forwards" and "content-type" headers
                // so as not to duplicate them; this is important because
                // some clients consider messages with duplicate single-valued
                // headers as malformed and may silently ignore invites
                match header {
                    crate::sip::Header::MaxForwards(_) | crate::sip::Header::ContentType(_) => {
                        request.headers.unique_push(header.clone())
                    }
                    _ => request.headers.push(header.clone()),
                }
            }
        }
        if let Some(config) = self.inner.session_timer_config.lock().as_ref() {
            config.initial(&mut request);
        }
        Ok(request)
    }

    /// Admit an initial INVITE owned by the stack. Dropping either observer does
    /// not cancel it; use the returned operation's explicit cancellation method.
    /// `wait()` preserves the received final response even if ACK/storage fails.
    pub fn start_invite(
        self: &Arc<Self>,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(
        InviteDialog,
        super::initial_operation::InitialInviteOperation,
    )> {
        let (dialog, _driver, operation) = self.spawn_initial_invite(opt, state_sender)?;
        Ok((dialog, operation))
    }

    fn spawn_initial_invite(
        self: &Arc<Self>,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(
        InviteDialog,
        crate::platform::JoinHandle<InviteAsyncResult>,
        super::initial_operation::InitialInviteOperation,
    )> {
        let progress = opt.provisional_responses.clone();
        let (dialog, mut tx) = self.create_client_invite_dialog(opt, state_sender)?;
        let control = super::initial_operation::InitialControl::new(dialog.clone());
        *control.trace.provisional.lock() = progress;
        *dialog.inner.initial_operation.lock() = Arc::downgrade(&control);
        tx.operation = Some(control.trace.clone());
        tx.force_success_ack = true;
        let operation = super::initial_operation::InitialInviteOperation::new(control.clone());
        let id0 = dialog.id();

        // 1) register early key (so in-dialog requests can be matched)
        self.inner
            .dialogs
            .insert(id0.clone(), Dialog::Invite(dialog.clone()));

        let inner = self.inner.clone();
        let dialog_clone = dialog.clone();

        // Own the guard before spawning so an abort before the first poll
        // also removes the early registry entry.
        let registry_guard = InitialRegistryGuard {
            layer: inner.clone(),
            id: id0.clone(),
            completed: false,
        };
        let owned_guard = super::initial_operation::InitialDriverGuard(control.clone());
        let handle = crate::platform::spawn_with_result(async move {
            let _owned_guard = owned_guard;
            let mut registry_guard = registry_guard;
            let r = {
                if dialog_clone
                    .inner
                    .cancel_requested
                    .load(core::sync::atomic::Ordering::SeqCst)
                {
                    control
                        .suppressed
                        .store(true, core::sync::atomic::Ordering::SeqCst);
                    control
                        .trace
                        .failed(super::operation::OperationError::CancelledBeforeSend);
                    dialog_clone.inner.transition(DialogState::Terminated(
                        dialog_clone.id(),
                        TerminatedReason::UacCancel,
                    ));
                    Ok((dialog_clone.id(), None))
                } else {
                    dialog_clone
                        .process_invite_with_registration(&mut tx, |new_id| {
                            inner.dialogs.with_mut(|dialogs| {
                                dialogs.remove(&id0);
                                dialogs
                                    .insert(new_id.clone(), Dialog::Invite(dialog_clone.clone()));
                            });
                        })
                        .boxed()
                        .await
                }
            };
            registry_guard.completed = true;
            if !dialog_clone.inner.is_confirmed() {
                inner.dialogs.remove(&id0);
            }

            match &r {
                Ok((new_id, resp_opt)) => {
                    let is_2xx = resp_opt
                        .as_ref()
                        .map(|resp| {
                            resp.status_code.kind() == crate::sip::StatusCodeKind::Successful
                        })
                        .unwrap_or(false);

                    if resp_opt.is_some() {
                        // The full key was registered before ACK. Do not
                        // resurrect a dialog the application already removed.
                        // RFC 6026 §7.2: keep draining the Accepted transaction
                        // until Timer M ends it (re-ACKs retransmitted 2xx,
                        // observes forked 2xx, and detaches it from the
                        // endpoint's table). See do_invite for the rationale.
                        let confirmed_id = new_id.clone();
                        let confirmed_tag = if is_2xx {
                            new_id.remote_tag.clone()
                        } else {
                            String::new()
                        };
                        let forked_dlg = dialog_clone.clone();
                        crate::platform::spawn(async move {
                            let mut seen_forks: Vec<String> = Vec::new();
                            while let Some(msg) = tx.receive().await {
                                forked_dlg
                                    .end_forked_branch(&msg, &confirmed_tag, &mut seen_forks)
                                    .await;
                            }
                            debug!(id = %confirmed_id, "accepted transaction drained (Timer M expired)");
                        });
                    } else {
                        // Release an unsent INVITE reservation before recording
                        // completion, including cancellation before first poll.
                        drop(tx);
                    }
                }
                Err(e) => {
                    debug!(%id0, error = %e, "async invite failed");
                    drop(tx);
                }
            }

            {
                let mut result = match &r {
                    Ok((_, response)) => Ok(response.clone()),
                    Err(error) => Err(crate::Error::Error(error.to_string())),
                };
                // The INVITE drainer already owns tx, so repeated 2xx are ACKed
                // while termination of the cancelled session is in progress.
                if dialog_clone
                    .inner
                    .cancel_requested
                    .load(core::sync::atomic::Ordering::SeqCst)
                {
                    if let Ok((_, Some(response))) = &r {
                        if response.status_code.kind() == crate::sip::StatusCodeKind::Successful {
                            if let Err(error) = dialog_clone.bye_2xx_after_cancel(response).await {
                                result = Err(error);
                            }
                        }
                    }
                }
                control.release_operation();
                let saved = dialog_clone
                    .inner
                    .checkpoint(
                        super::persistence::DialogCheckpointReason::SignalingCompleted,
                        None,
                    )
                    .await;
                control
                    .trace
                    .complete(&dialog_clone.inner, saved.and(result));
                control.finished.cancel();
            }
            r
        });

        Ok((dialog, handle, operation))
    }
    pub fn create_client_invite_dialog(
        &self,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(InviteDialog, Transaction)> {
        let mut request = self.make_invite_request(&opt)?;
        request.body = opt.offer.unwrap_or_default();
        request
            .headers
            .unique_push(crate::sip::Header::ContentLength(
                (request.body.len() as u32).into(),
            ));
        let key = TransactionKey::from_request(&request, TransactionRole::Client)?;
        let mut tx = Transaction::new_client(key, request.clone(), self.endpoint.clone(), None);

        if opt.destination.is_some() {
            tx.destination = opt.destination;
        } else {
            if let Some(route) = tx.original.route_header() {
                if let Ok(first_route) = route.typed() {
                    tx.destination = SipAddr::try_from(&first_route.uri).ok();
                }
            }
        }

        let id = DialogId::try_from(&tx)?;

        let local_contact = request
            .contact_header()
            .ok()
            .and_then(|c| c.typed().ok().map(|ct| ct.uri))
            .or(Some(opt.contact));

        let mut dlg_inner = DialogInner::new(
            TransactionRole::Client,
            id.clone(),
            request.clone(),
            self.endpoint.clone(),
            state_sender,
            opt.credential,
            local_contact,
            tx.tu_sender.clone(),
        )?;
        dlg_inner.owned_state = true;

        if let Some(destination) = &tx.destination {
            let uri = destination.clone().into();
            *dlg_inner.remote_uri.lock() = uri;
        }
        let dialog = InviteDialog::from_inner(Arc::new(dlg_inner));
        dialog.inner.lifecycle.lock().sender = self.inner.lifecycle_sender.lock().clone();
        dialog
            .inner
            .configure_persistence(self.inner.persistence_config.lock().clone());
        dialog.inner.bind_persistence(&tx);
        dialog.inner.reserve_invite(&tx)?;
        *dialog.inner.reliable_config.lock() = self.inner.reliable_config.lock().clone();
        if let Some(config) = self.inner.session_timer_config.lock().clone() {
            super::session_timer::SessionTimer::install(&dialog.inner, config);
        }
        Ok((dialog, tx))
    }
}

/// Registry cleanup for a stopped managed driver. No cancellation-on-observer-drop
/// policy lives here; the initial operation reports DriverStopped independently.
struct InitialRegistryGuard {
    layer: DialogLayerInnerRef,
    id: DialogId,
    completed: bool,
}
impl Drop for InitialRegistryGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.layer.dialogs.remove(&self.id);
        }
    }
}

pub(super) async fn bye_if_2xx(client_dialog: &InviteDialog, resp: Option<Response>) {
    if let Some(resp) = resp.filter(|r| r.status_code.kind() == StatusCodeKind::Successful) {
        info!(id = %client_dialog.id(), "2xx after CANCEL, sending BYE");
        if let Err(e) = client_dialog.bye_2xx_after_cancel(&resp).await {
            warn!(id = %client_dialog.id(), error = %e, "BYE after CANCEL failed");
        }
    }
}

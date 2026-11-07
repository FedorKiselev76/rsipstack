//! RFC 3903 event publication. PUBLISH does not establish a SIP dialog.
//! Storage, authorization and event-state composition remain application policy.
use super::exchange::{self, DriverGuard, ExchangeOperation, Queue, Trace};
use super::*;
use crate::{
    platform::{self, sync::Mutex, Instant},
    sip::{uri::ParamsExt, HeadersExt, Uri},
    transaction::{endpoint::EndpointInnerRef, key::TransactionKey, make_tag},
    transport::SipAddr,
};
use core::time::Duration;

#[derive(Clone)]
pub struct PublicationOptions {
    pub caller: Uri,
    pub resource: Uri,
    pub event: String,
    pub credential: Option<Credential>,
    pub destination: Option<SipAddr>,
    pub route_set: Vec<crate::sip::Route>,
}
impl PublicationOptions {
    pub fn new(caller: Uri, resource: Uri, event: impl Into<String>) -> Self {
        Self {
            caller,
            resource,
            event: event.into(),
            credential: None,
            destination: None,
            route_set: Vec::new(),
        }
    }
}
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct PublicationStatus {
    pub etag: Option<String>,
    pub expires: u32,
    pub expires_at: Option<Instant>,
    /// A timeout, transport failure or invalid success response leaves the remote result uncertain.
    pub unknown_outcome: bool,
}
struct State {
    status: PublicationStatus,
    cseq: u32,
    call_id: Option<crate::sip::CallId>,
}
struct Inner {
    endpoint: EndpointInnerRef,
    options: PublicationOptions,
    from: crate::sip::typed::From,
    state: Mutex<State>,
    queue: Arc<Queue>,
    control: Arc<control::Control>,
}
/// A publication context, never an INVITE dialog. Commands are serialized through
/// their final response and ETag update. Dropping observers does not stop them.
#[derive(Clone)]
pub struct Publication(Arc<Inner>);
enum Command {
    Publish(Body, u32),
    Refresh(u32),
    Remove,
}
impl Publication {
    pub(super) fn new(
        endpoint: EndpointInnerRef,
        options: PublicationOptions,
        control: Arc<control::Control>,
    ) -> crate::Result<Self> {
        if !exchange::token(&options.event) {
            return Err(crate::Error::Error(
                "invalid publication event package".into(),
            ));
        }
        let queue = control.publication_queue(options.resource.to_string());
        let from = crate::sip::typed::From {
            display_name: None,
            uri: options.caller.clone(),
            params: vec![],
        }
        .with_tag(make_tag());
        Ok(Self(Arc::new(Inner {
            endpoint,
            options,
            from,
            state: Mutex::new(State {
                status: PublicationStatus::default(),
                cseq: 0,
                call_id: None,
            }),
            queue,
            control,
        })))
    }
    pub fn status(&self) -> PublicationStatus {
        let mut status = self.0.state.lock().status.clone();
        if status
            .expires_at
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            status.etag = None;
            status.expires = 0;
        }
        status
    }
    /// Create or modify state. A queued command reads the latest acknowledged ETag
    /// after its predecessor finishes, never at queue admission.
    pub fn start_publish(
        &self,
        body: Body,
        expires: u32,
    ) -> core::result::Result<ExchangeOperation, AdmissionError> {
        if expires == 0 || body.as_bytes().is_empty() {
            return Err(AdmissionError::InvalidRequest(
                "publish requires nonempty state and positive expiry".into(),
            ));
        }
        self.start(Command::Publish(body, expires))
    }
    pub fn start_refresh(
        &self,
        expires: u32,
    ) -> core::result::Result<ExchangeOperation, AdmissionError> {
        if expires == 0 {
            return Err(AdmissionError::InvalidRequest(
                "refresh requires positive expiry".into(),
            ));
        }
        self.start(Command::Refresh(expires))
    }
    /// Explicit cleanup remains available after layer admission closes.
    pub fn start_remove(&self) -> core::result::Result<ExchangeOperation, AdmissionError> {
        self.start(Command::Remove)
    }
    fn start(&self, command: Command) -> core::result::Result<ExchangeOperation, AdmissionError> {
        let _gate = if matches!(command, Command::Remove) {
            None
        } else {
            Some(self.0.control.admit()?)
        };
        let ticket = self.0.queue.reserve();
        let trace = Trace::new();
        let observer = trace.clone();
        let guard = DriverGuard(trace.clone());
        let publication = self.clone();
        let tracking_id = self.0.control.track_context(
            None,
            "publication",
            Some(self.0.options.resource.to_string()),
            async move {
                let _guard = guard;
                ticket.wait().await;
                let result = publication.drive(command, &trace).await;
                if matches!(
                    result,
                    Err(OperationError::Timeout | OperationError::Transport(_))
                ) {
                    publication.0.state.lock().status.unknown_outcome = true;
                }
                // No successor may construct SIP-If-Match before state is updated.
                trace.finish(result);
                drop(ticket);
            },
        );
        Ok(ExchangeOperation {
            trace: observer,
            tracking_id,
        })
    }
    fn next_cseq(&self) -> crate::Result<u32> {
        let mut state = self.0.state.lock();
        state.cseq = state
            .cseq
            .checked_add(1)
            .filter(|n| *n < (1 << 31))
            .ok_or_else(|| crate::Error::Error("publication CSeq exhausted".into()))?;
        Ok(state.cseq)
    }
    async fn drive(
        &self,
        command: Command,
        trace: &Trace,
    ) -> core::result::Result<(), OperationError> {
        let tag = self.status().etag;
        let (body, expires) = match command {
            Command::Publish(body, expires) => (Some(body), expires),
            Command::Refresh(expires) => {
                if tag.is_none() {
                    return Err(exchange::protocol("no live publication to refresh"));
                }
                (None, expires)
            }
            Command::Remove => {
                if tag.is_none() {
                    return Err(exchange::protocol("no live publication to remove"));
                }
                (None, 0)
            }
        };
        let options = &self.0.options;
        let (uri, routes) =
            crate::sip::routing::encode(options.resource.clone(), &options.route_set)
                .map_err(exchange::protocol)?;
        let hop = crate::sip::routing::next_hop(&options.resource, &options.route_set)
            .map_err(exchange::protocol)?;
        let transport = options
            .destination
            .as_ref()
            .and_then(|d| d.r#type)
            .or_else(|| hop.transport().cloned())
            .or_else(|| {
                (hop.scheme == Some(crate::sip::Scheme::Sips)).then_some(crate::sip::Transport::Tls)
            });
        let listeners = self.0.endpoint.transport_layer.get_listen_addrs();
        let listener = transport.and_then(|transport| {
            listeners
                .iter()
                .find(|a| a.r#type.unwrap_or_default() == transport)
                .cloned()
                .or_else(|| {
                    let mut listener = listeners.first()?.clone();
                    listener.r#type = Some(transport);
                    Some(listener)
                })
        });
        let via = self
            .0
            .endpoint
            .get_via(listener, None)
            .map_err(exchange::protocol)?;
        let to = crate::sip::typed::To {
            display_name: None,
            uri: options.resource.clone(),
            params: vec![],
        };
        let call_id = self.0.state.lock().call_id.clone();
        let mut request = self.0.endpoint.make_request(
            Method::Publish,
            uri,
            via,
            self.0.from.clone(),
            to,
            self.next_cseq().map_err(exchange::protocol)?,
            call_id,
        );
        self.0.state.lock().call_id = Some(
            request
                .call_id_header()
                .map_err(exchange::protocol)?
                .clone(),
        );
        request
            .headers
            .extend(routes.into_iter().map(Header::Route).collect());
        request
            .headers
            .push(Header::Event(options.event.clone().into()));
        request.headers.push(Header::Expires(expires.into()));
        if let Some(tag) = tag {
            request
                .headers
                .push(Header::Other("SIP-If-Match".into(), tag));
        }
        if let Some(body) = body {
            request
                .headers
                .push(Header::ContentType(body.content_type().into()));
            request.body = body.into_bytes();
        }
        request
            .headers
            .push(Header::ContentLength((request.body.len() as u32).into()));
        let key = TransactionKey::from_request(&request, TransactionRole::Client)
            .map_err(exchange::protocol)?;
        let mut tx = Transaction::new_client(key, request, self.0.endpoint.clone(), None);
        tx.destination = options.destination.clone();
        tx.dialog_route_set = Some(options.route_set.clone());
        let response =
            exchange::send(tx, options.credential.as_ref(), trace, || self.next_cseq()).await?;
        if response.status_code.code() == 412 {
            self.0.state.lock().status = PublicationStatus::default();
        } else if (200..300).contains(&response.status_code.code()) {
            if expires == 0 {
                self.0.state.lock().status = PublicationStatus::default();
            } else {
                // The old tag may have been replaced even when this response is malformed.
                self.0.state.lock().status = PublicationStatus {
                    unknown_outcome: true,
                    ..Default::default()
                };
                let etag = exchange::header(&response, "SIP-ETag")
                    .filter(|t| exchange::token(t))
                    .ok_or_else(|| {
                        exchange::protocol("successful PUBLISH lacks a valid SIP-ETag")
                    })?;
                let granted = exchange::header(&response, "Expires")
                    .and_then(|v| v.parse::<u32>().ok())
                    .filter(|v| *v <= expires)
                    .ok_or_else(|| exchange::protocol("successful PUBLISH has invalid Expires"))?;
                self.0.state.lock().status = PublicationStatus {
                    etag: Some(etag),
                    expires: granted,
                    expires_at: Some(Instant::now() + Duration::from_secs(granted.into())),
                    unknown_outcome: false,
                };
            }
        }
        Ok(())
    }
}

/// One incoming PUBLISH transaction. The application owns the publication store,
/// checks SIP-If-Match atomically there, and chooses the ETag and granted expiry.
/// Dropping all responders without a final response results in 501.
#[derive(Clone)]
pub struct IncomingPublication {
    request: Request,
    endpoint: EndpointInnerRef,
    sender: crate::platform::mpsc::UnboundedSender<
        crate::dialog::response_operation::ResponseSubmission,
    >,
    admitted: Arc<Mutex<bool>>,
}
impl IncomingPublication {
    #[cfg_attr(not(feature = "platform-tokio"), allow(unused_mut))]
    pub(super) fn new(
        mut transaction: Transaction,
        control: Arc<control::Control>,
    ) -> crate::Result<Self> {
        if transaction.role() != TransactionRole::Server
            || transaction.original.method != Method::Publish
        {
            return Err(crate::Error::Error("expected incoming PUBLISH".into()));
        }
        let (sender, mut receiver) = crate::platform::mpsc::unbounded_channel();
        let incoming = Self {
            request: transaction.original.clone(),
            endpoint: transaction.endpoint_inner.clone(),
            sender,
            admitted: Arc::new(Mutex::new(false)),
        };
        control.track_context(
            None,
            "incoming-publication",
            Some(transaction.original.uri.to_string()),
            async move {
                let timeout = transaction.endpoint_inner.option.t1x64;
                match platform::timeout(timeout, receiver.recv()).await {
                    Ok(Some(submission)) => {
                        let _ = transaction.respond_submission(submission).await;
                    }
                    _ => {
                        let _ = transaction.reply(StatusCode::NotImplemented).await;
                    }
                }
                while transaction.receive().await.is_some() {}
            },
        );
        Ok(incoming)
    }
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn event(&self) -> Option<String> {
        exchange::header(&self.request, "Event")
    }
    pub fn if_match(&self) -> Option<String> {
        exchange::header(&self.request, "SIP-If-Match")
    }
    pub fn expires(&self) -> Option<u32> {
        exchange::header(&self.request, "Expires").and_then(|v| v.parse().ok())
    }
    pub fn start_accept(
        &self,
        etag: &str,
        expires: u32,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        if !exchange::token(etag) {
            return Err(ResponseError::Protocol("invalid SIP-ETag".into()));
        }
        self.start_response(
            StatusCode::OK,
            Some(vec![
                Header::Other("SIP-ETag".into(), etag.into()),
                Header::Expires(expires.into()),
            ]),
        )
    }
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        if !(200..700).contains(&status.code()) {
            return Err(ResponseError::Protocol(
                "expected a final SIP response".into(),
            ));
        }
        let mut admitted = self.admitted.lock();
        if *admitted {
            return Err(ResponseError::FinalAlreadyAdmitted);
        }
        let mut response = self.endpoint.make_response(&self.request, status, None);
        if let Some(headers) = headers {
            for header in headers {
                if [
                    "via",
                    "from",
                    "to",
                    "call-id",
                    "cseq",
                    "content-length",
                    "v",
                    "f",
                    "t",
                    "i",
                    "l",
                ]
                .iter()
                .any(|name| header.name().eq_ignore_ascii_case(name))
                {
                    return Err(ResponseError::Protocol(
                        "response identity and framing are stack-owned".into(),
                    ));
                }
                response.headers.push(header);
            }
        }
        let (receipt, producer) = ResponseReceipt::pair();
        producer
            .trace()
            .acknowledge(Err(ResponseError::AckNotApplicable));
        self.sender
            .send(crate::dialog::response_operation::ResponseSubmission {
                response,
                producer: Some(producer),
                initial_owner: None,
            })
            .map_err(|_| ResponseError::Closed)?;
        *admitted = true;
        Ok(receipt)
    }
}
impl DialogLayer {
    pub fn publication(&self, options: PublicationOptions) -> crate::Result<Publication> {
        let _gate = self
            .1
            .admit()
            .map_err(|e| crate::Error::Error(e.to_string()))?;
        Publication::new(self.0.endpoint.clone(), options, self.1.clone())
    }
    /// Receiving a PUBLISH is protocol work; application overload/rejection policy
    /// remains available while normal outbound command admission is closed.
    pub fn incoming_publication(
        &self,
        transaction: Transaction,
    ) -> crate::Result<IncomingPublication> {
        IncomingPublication::new(transaction, self.1.clone())
    }
}

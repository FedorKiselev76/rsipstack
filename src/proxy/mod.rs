#![doc = include_str!("../../PROXY-API-EXPERIMENT.md")]
/// Owned proxy operations use the same generation namespace as managed dialogs.
/// These are the existing owned proxy types; the root paths remain aliases for
/// compatibility. No dialog or B2BUA is created by this interface.
pub mod v2managed {
    pub use super::{
        ProxyConfig, ProxyError, ProxyLayer, ProxyOperation, ProxyProgress, ProxyTarget,
    };
}
mod routing;
pub use routing::{ProxyConfig, ProxyTarget};

use crate::{
    dialog::response_operation::{ResponseReceipt, ResponseSubmission},
    platform::{self, sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode, Uri},
    transaction::{
        endpoint::EndpointInnerRef,
        key::{TransactionKey, TransactionRole},
        transaction::TransactionEvent,
        Transaction,
    },
    transport::SipAddr,
};
use core::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    #[error("proxy rejected the request: {0}")]
    Rejected(String),
    #[error("proxy signaling failed: {0}")]
    Signaling(String),
    #[error("proxy driver stopped before retirement")]
    DriverStopped,
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ProxyProgress {
    /// The prepared outbound request, not a transport-delivery guarantee.
    pub request: Option<Request>,
    /// First final response whose upstream send completed.
    pub first_final: Option<Response>,
    /// Most recent response whose upstream send completed.
    pub last_response: Option<Response>,
    pub responses_forwarded: u64,
    pub cancel_submitted: bool,
}
struct Observation {
    progress: ProxyProgress,
    result: Option<core::result::Result<ProxyProgress, ProxyError>>,
}
struct Trace {
    observation: Mutex<Observation>,
    final_response: CancellationToken,
    done: CancellationToken,
}
impl Trace {
    fn response(&self, response: Response) {
        let mut state = self.observation.lock();
        state.progress.responses_forwarded += 1;
        if response.status_code.code() >= 200 && state.progress.first_final.is_none() {
            state.progress.first_final = Some(response.clone());
            self.final_response.cancel();
        }
        state.progress.last_response = Some(response);
    }
    fn finish(&self, result: core::result::Result<(), ProxyError>) {
        let mut state = self.observation.lock();
        if state.result.is_none() {
            state.result = Some(result.map(|_| state.progress.clone()));
            self.done.cancel();
            self.final_response.cancel();
        }
    }
}
struct DriverGuard(Arc<Trace>);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.finish(Err(ProxyError::DriverStopped));
    }
}

#[derive(Clone)]
#[must_use]
pub struct ProxyOperation {
    trace: Arc<Trace>,
}
impl ProxyOperation {
    pub fn progress(&self) -> ProxyProgress {
        self.trace.observation.lock().progress.clone()
    }
    /// Wait for the first final response to be sent upstream. This is not a
    /// transaction-retirement barrier; retransmissions and forked 2xx may follow.
    /// ACK forwarding completes without a response and returns None.
    pub async fn wait_final_response(&self) -> core::result::Result<Option<Response>, ProxyError> {
        self.trace.final_response.cancelled().await;
        let state = self.trace.observation.lock();
        if let Some(response) = &state.progress.first_final {
            return Ok(Some(response.clone()));
        }
        match &state.result {
            Some(Err(error)) => Err(error.clone()),
            _ => Ok(None),
        }
    }
    /// Wait for both transaction owners and any generated CANCEL to retire.
    /// Endpoint shutdown can interrupt protocol retention and is not graceful
    /// drain. Dropping this future or handle does not abort the owners.
    pub async fn wait(&self) -> core::result::Result<ProxyProgress, ProxyError> {
        self.trace.done.cancelled().await;
        self.trace
            .observation
            .lock()
            .result
            .clone()
            .expect("proxy result precedes completion notification")
    }
}

#[derive(Clone)]
pub struct ProxyLayer {
    endpoint: EndpointInnerRef,
    config: ProxyConfig,
}
impl ProxyLayer {
    pub fn new(endpoint: EndpointInnerRef, config: ProxyConfig) -> Self {
        endpoint.proxy_ack_dispatch.store(true, Ordering::Relaxed);
        Self { endpoint, config }
    }
    /// Admit one authorized incoming request. Identity, Contact, SDP and
    /// end-to-end extension headers are preserved. Validation failures receive
    /// a local SIP response (except ACK, which never receives a response).
    pub fn forward(&self, incoming: Transaction, target: ProxyTarget) -> ProxyOperation {
        let trace = Arc::new(Trace {
            observation: Mutex::new(Observation {
                progress: ProxyProgress::default(),
                result: None,
            }),
            final_response: CancellationToken::new(),
            done: CancellationToken::new(),
        });
        let operation = ProxyOperation {
            trace: trace.clone(),
        };
        let layer = self.clone();
        let guard = DriverGuard(trace.clone());
        platform::spawn(async move {
            let _guard = guard;
            let shutdown = layer.endpoint.shutdown_token();
            // Explicit endpoint shutdown is a forced boundary, unlike dropping
            // an observer. Report interruption, never successful retirement.
            let result = match platform::select2(
                core::pin::pin!(layer.drive(incoming, target, trace.clone())),
                core::pin::pin!(shutdown.cancelled()),
            )
            .await
            {
                platform::select::Either::A(result) => result,
                platform::select::Either::B(_) => Err(ProxyError::DriverStopped),
            };
            trace.finish(result);
        });
        operation
    }

    async fn drive(
        &self,
        mut incoming: Transaction,
        target: ProxyTarget,
        trace: Arc<Trace>,
    ) -> core::result::Result<(), ProxyError> {
        if !Arc::ptr_eq(&incoming.endpoint_inner, &self.endpoint) {
            return Err(ProxyError::Rejected(
                "incoming transaction belongs to another endpoint".into(),
            ));
        }
        if incoming.role() != TransactionRole::Server {
            return Err(ProxyError::Rejected(
                "proxy forwarding requires an incoming server transaction".into(),
            ));
        }
        incoming.proxy_forwarding = true;
        let method = incoming.original.method.clone();
        let (request, destination) =
            match routing::prepare(&incoming.original, target, &self.config, &self.endpoint) {
                Ok(prepared) => prepared,
                Err((status, reason)) => {
                    if method != Method::Ack {
                        let unsupported: Vec<_> = incoming
                            .original
                            .headers
                            .iter()
                            .filter_map(|h| match h {
                                Header::ProxyRequire(value)
                                    if status == StatusCode::BadExtension =>
                                {
                                    Some(Header::Unsupported(value.value().into()))
                                }
                                _ => None,
                            })
                            .collect();
                        incoming
                            .reply_with(status, unsupported, None)
                            .await
                            .map_err(signal)?;
                        if let Some(response) = &incoming.last_response {
                            trace.response(response.clone());
                        }
                        while incoming.receive().await.is_some() {}
                    }
                    return Err(ProxyError::Rejected(reason));
                }
            };
        trace.observation.lock().progress.request = Some(request.clone());
        if method == Method::Ack {
            self.endpoint
                .send_stateless(request, destination)
                .await
                .map_err(signal)?;
            return Ok(());
        }
        let key =
            TransactionKey::from_request(&request, TransactionRole::Client).map_err(signal)?;
        let mut outgoing =
            Transaction::new_client(key, request.clone(), self.endpoint.clone(), None);
        outgoing.proxy_forwarding = true;
        outgoing.destination = Some(destination.clone());
        let upstream = incoming.tu_sender.clone();
        let original = incoming.original.clone();
        let control = Arc::new(Control::default());
        outgoing.proxy_timeout_cancel = Some(control.cancel.clone());
        let server_control = control.clone();
        let server = async move {
            while let Some(message) = incoming.receive().await {
                if let SipMessage::Request(request) = message {
                    if request.method == Method::Cancel {
                        *server_control.cancel_headers.lock() = request
                            .headers
                            .iter()
                            .filter(|h| matches!(h, Header::Reason(_)))
                            .cloned()
                            .collect();
                        server_control.cancel.cancel();
                    }
                }
            }
        };
        let cancel_control = control.clone();
        let cancel_trace = trace.clone();
        let cancellation = async {
            platform::select2(
                core::pin::pin!(cancel_control.cancel.cancelled()),
                core::pin::pin!(cancel_control.finished.cancelled()),
            )
            .await;
            if !cancel_control.cancel.is_cancelled() || method != Method::Invite {
                return Ok(());
            }
            platform::select2(
                core::pin::pin!(cancel_control.provisional.cancelled()),
                core::pin::pin!(cancel_control.finished.cancelled()),
            )
            .await;
            if cancel_control.final_seen.load(Ordering::SeqCst)
                || cancel_control.finished.is_cancelled()
            {
                return Ok(());
            }
            let mut cancel = request.clone();
            cancel.method = Method::Cancel;
            let via = cancel
                .via_header()
                .map_err(signal)?
                .first_value()
                .map_err(signal)?;
            // A generated CANCEL is hop-by-hop and has precisely one Via
            // (RFC 3261 9.1), while keeping this branch's Route and identity.
            cancel.headers.retain(|h| {
                matches!(
                    h,
                    Header::From(_)
                        | Header::To(_)
                        | Header::CallId(_)
                        | Header::CSeq(_)
                        | Header::Route(_)
                        | Header::MaxForwards(_)
                )
            });
            cancel.headers.push_front(Header::Via(via));
            cancel
                .headers
                .extend(cancel_control.cancel_headers.lock().clone());
            cancel
                .cseq_header_mut()
                .map_err(signal)?
                .mut_method(Method::Cancel)
                .map_err(signal)?;
            cancel
                .headers
                .retain(|h| !matches!(h, Header::ContentType(_) | Header::ContentLength(_)));
            cancel.headers.unique_push(Header::ContentLength(0.into()));
            cancel.body.clear();
            let key =
                TransactionKey::from_request(&cancel, TransactionRole::Client).map_err(signal)?;
            let mut tx = Transaction::new_client(key, cancel, self.endpoint.clone(), None);
            tx.destination = Some(destination);
            tx.send().await.map_err(signal)?;
            cancel_trace.observation.lock().progress.cancel_submitted = true;
            while tx.receive().await.is_some() {}
            Ok::<_, ProxyError>(())
        };
        let client = async {
            let result = async {
                outgoing.send().await.map_err(signal)?;
                while let Some(message) = outgoing.receive().await {
                    let SipMessage::Response(mut response) = message else {
                        continue;
                    };
                    if response.status_code.code() >= 200 {
                        control.final_seen.store(true, Ordering::SeqCst);
                    }
                    control.provisional.cancel();
                    if response.status_code == StatusCode::Trying {
                        continue;
                    }
                    if outgoing.last_response_origin()
                        == crate::dialog::operation::ResponseOrigin::Local
                        && response
                            .to_header()
                            .and_then(|h| h.tag())
                            .map_err(signal)?
                            .is_none()
                    {
                        let tag = trace
                            .observation
                            .lock()
                            .progress
                            .last_response
                            .as_ref()
                            .and_then(|r| r.to_header().ok()?.tag().ok().flatten())
                            .unwrap_or_else(crate::transaction::make_tag);
                        response
                            .to_header_mut()
                            .map_err(signal)?
                            .mut_tag(tag)
                            .map_err(signal)?;
                    }
                    routing::pop_via(&mut response, &request).map_err(signal)?;
                    let (receipt, producer) = ResponseReceipt::pair();
                    producer.trace().acknowledge(Err(
                        crate::dialog::response_operation::ResponseError::AckNotApplicable,
                    ));
                    upstream
                        .send(TransactionEvent::RespondTracked(ResponseSubmission {
                            response: response.clone(),
                            producer: Some(producer),
                            initial_owner: None,
                        }))
                        .map_err(signal)?;
                    receipt.wait_sent().await.map_err(signal)?;
                    trace.response(response);
                }
                Ok::<_, ProxyError>(())
            }
            .await;
            if result.is_err() && !control.final_seen.load(Ordering::SeqCst) {
                let response = self
                    .endpoint
                    .make_response(&original, StatusCode::BadGateway, None);
                let _ = upstream.send(TransactionEvent::Respond(response));
            }
            control.finished.cancel();
            result
        };
        let (_, client, cancellation) = futures::join!(server, client, cancellation);
        client.and(cancellation)
    }
}

struct Control {
    cancel_headers: Mutex<Vec<Header>>,
    cancel: CancellationToken,
    provisional: CancellationToken,
    finished: CancellationToken,
    final_seen: AtomicBool,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            cancel_headers: Mutex::new(Vec::new()),
            cancel: CancellationToken::new(),
            provisional: CancellationToken::new(),
            finished: CancellationToken::new(),
            final_seen: AtomicBool::new(false),
        }
    }
}
fn signal(error: impl core::fmt::Display) -> ProxyError {
    ProxyError::Signaling(error.to_string())
}

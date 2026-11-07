#![cfg(feature = "platform-tokio")]
#![deny(deprecated)]
use rsipstack::{
    dialog::v2managed::{AdmissionError, Body, DialogLayer, PublicationOptions},
    sip::{Header, HeadersExt, Request, SipMessage, StatusCode},
    transaction::{
        endpoint::{EndpointInnerRef, EndpointOption},
        EndpointBuilder, Transaction,
    },
    transport::{udp::UdpConnection, TransportLayer},
};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Node {
    layer: DialogLayer,
    endpoint: EndpointInnerRef,
    address: SocketAddr,
    incoming: mpsc::UnboundedReceiver<Transaction>,
    stop: CancellationToken,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Node {
    async fn new() -> Result<Self> {
        let stop = CancellationToken::new();
        let transport = TransportLayer::new(stop.clone());
        let connection =
            UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone()))
                .await?;
        let address = connection.get_addr().get_socketaddr()?;
        transport.add_transport(connection.into());
        let endpoint = EndpointBuilder::new()
            .with_transport_layer(transport)
            .with_cancel_token(stop.clone())
            .with_timer_interval(Duration::from_millis(5))
            .with_option(EndpointOption {
                t1: Duration::from_millis(100),
                t2: Duration::from_millis(100),
                t4: Duration::from_millis(20),
                t1x64: Duration::from_millis(600),
                ..Default::default()
            })
            .build();
        let incoming = endpoint.incoming_transactions()?;
        let inner = endpoint.inner.clone();
        tokio::spawn(async move { endpoint.serve().await });
        Ok(Self {
            layer: DialogLayer::new(inner.clone()),
            endpoint: inner,
            address,
            incoming,
            stop,
        })
    }
    fn options(&self, target: SocketAddr) -> Result<PublicationOptions> {
        Ok(PublicationOptions::new(
            format!("sip:alice@{}", self.address).try_into()?,
            format!("sip:presence@{target}").try_into()?,
            "presence",
        ))
    }
}
fn value(message: &impl HeadersExt, name: &str) -> Option<String> {
    message
        .headers()
        .iter()
        .find(|h| h.name().eq_ignore_ascii_case(name))
        .and_then(|h| {
            h.to_string()
                .split_once(':')
                .map(|(_, v)| v.trim().to_owned())
        })
}
async fn request(peer: &UdpSocket) -> Result<(Request, SocketAddr)> {
    let mut bytes = [0; 8192];
    let (n, address) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes)).await??;
    match SipMessage::try_from(&bytes[..n])? {
        SipMessage::Request(r) => Ok((r, address)),
        _ => Err("expected request".into()),
    }
}
async fn answer(
    node: &Node,
    peer: &UdpSocket,
    request: &Request,
    address: SocketAddr,
    code: StatusCode,
    tag: &str,
    expires: u32,
) -> Result {
    let mut response = node.endpoint.make_response(request, code, None);
    response
        .to_header_mut()?
        .mut_tag("publication-response-tag".into())?;
    response
        .headers
        .push(Header::Other("SIP-ETag".into(), tag.into()));
    response.headers.push(Header::Expires(expires.into()));
    // PUBLISH must not learn either a dialog route set or a remote target.
    response
        .headers
        .push(Header::RecordRoute("<sip:invalid.example;lr>".into()));
    response
        .headers
        .push(Header::Contact("<sip:invalid.example>".into()));
    peer.send_to(response.to_string().as_bytes(), address)
        .await?;
    Ok(())
}
#[tokio::test]
async fn publication_serializes_etag_rotation_refresh_and_explicit_removal() -> Result {
    let node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let publication = node.layer.publication(node.options(peer.local_addr()?)?)?;
    let first = publication.start_publish(Body::new("application/pidf+xml", "<presence/>"), 600)?;
    let refresh = publication.start_refresh(600)?;
    let (initial, address) = request(&peer).await?;
    assert_eq!(value(&initial, "Event").as_deref(), Some("presence"));
    assert!(value(&initial, "SIP-If-Match").is_none());
    assert_eq!(initial.body, b"<presence/>");
    let mut bytes = [0; 8192];
    assert!(
        timeout(Duration::from_millis(25), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    answer(&node, &peer, &initial, address, StatusCode::OK, "v1", 300).await?;
    first
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    let (request, address) = request(&peer).await?;
    assert_eq!(value(&request, "SIP-If-Match").as_deref(), Some("v1"));
    assert!(request.body.is_empty());
    assert!(request.to_header()?.tag()?.is_none());
    assert!(request.route_headers().is_empty());
    assert_eq!(request.uri, initial.uri);
    assert_eq!(
        request.cseq_header()?.seq()?,
        initial.cseq_header()?.seq()? + 1
    );
    answer(&node, &peer, &request, address, StatusCode::OK, "v2", 240).await?;
    refresh
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    assert_eq!(publication.status().etag.as_deref(), Some("v2"));
    node.layer.close_admission();
    assert!(matches!(
        publication.start_refresh(600),
        Err(AdmissionError::LayerClosed)
    ));
    let remove = publication.start_remove()?;
    let (request, address) = self::request(&peer).await?;
    assert!(request.body.is_empty());
    assert_eq!(value(&request, "SIP-If-Match").as_deref(), Some("v2"));
    assert_eq!(value(&request, "Expires").as_deref(), Some("0"));
    answer(&node, &peer, &request, address, StatusCode::OK, "v2", 0).await?;
    remove
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    assert!(publication.status().etag.is_none());
    assert!(node.layer.drain(Duration::from_secs(1)).await.is_drained());
    Ok(())
}
#[tokio::test]
async fn incoming_publication_owns_transaction_and_dropped_responder_gets_501() -> Result {
    let sender = Node::new().await?;
    let mut receiver = Node::new().await?;
    let publication = sender
        .layer
        .publication(sender.options(receiver.address)?)?;
    let first = publication.start_publish(Body::new("application/pidf+xml", "<presence/>"), 600)?;
    let tx = timeout(Duration::from_secs(2), receiver.incoming.recv())
        .await?
        .ok_or("missing PUBLISH")?;
    assert!(tx.original.contact_header().is_err());
    let incoming = receiver.layer.incoming_publication(tx)?;
    assert_eq!(incoming.event().as_deref(), Some("presence"));
    let report = receiver.layer.drain(Duration::from_millis(20)).await;
    assert!(report.timed_out);
    let receipt = incoming.start_accept("server-tag", 300)?;
    receipt.wait_sent().await?;
    first
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    assert_eq!(publication.status().etag.as_deref(), Some("server-tag"));
    assert!(receiver
        .layer
        .drain(Duration::from_secs(1))
        .await
        .is_drained());
    let refresh = publication.start_refresh(600)?;
    let tx = timeout(Duration::from_secs(2), receiver.incoming.recv())
        .await?
        .ok_or("missing refresh")?;
    let incoming = receiver.layer.incoming_publication(tx)?;
    assert_eq!(incoming.if_match().as_deref(), Some("server-tag"));
    drop(incoming);
    let result = refresh.wait().await;
    assert_eq!(
        result.response().map(|r| r.status_code.clone()),
        Some(StatusCode::NotImplemented)
    );
    Ok(())
}

#[tokio::test]
async fn publication_queue_is_shared_by_resource_and_412_invalidates_only_its_context() -> Result {
    let node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let a = node.layer.publication(node.options(peer.local_addr()?)?)?;
    let b = node.layer.publication(node.options(peer.local_addr()?)?)?;
    let first = a.start_publish(Body::text("state-a"), 600)?;
    let second = b.start_publish(Body::text("state-b"), 600)?;
    let (initial, address) = request(&peer).await?;
    let mut bytes = [0; 8192];
    assert!(
        timeout(Duration::from_millis(25), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    answer(
        &node,
        &peer,
        &initial,
        address,
        StatusCode::OK,
        "tag-a",
        300,
    )
    .await?;
    first
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    let (next, address) = request(&peer).await?;
    assert_eq!(next.body, b"state-b");
    assert!(value(&next, "SIP-If-Match").is_none());
    answer(&node, &peer, &next, address, StatusCode::OK, "tag-b", 300).await?;
    second
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    let refresh = a.start_refresh(600)?;
    let (next, address) = request(&peer).await?;
    answer(
        &node,
        &peer,
        &next,
        address,
        StatusCode::from(412),
        "ignored",
        0,
    )
    .await?;
    let result = refresh.wait().await;
    assert_eq!(result.response().unwrap().status_code.code(), 412);
    assert!(a.status().etag.is_none());
    assert_eq!(b.status().etag.as_deref(), Some("tag-b"));
    Ok(())
}

#[tokio::test]
async fn publication_auth_retries_and_malformed_success_remain_observable() -> Result {
    let node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let mut options = node.options(peer.local_addr()?)?;
    options.credential = Some(rsipstack::dialog::v2managed::Credential {
        username: "alice".into(),
        password: "example-password".into(),
        realm: Some("example".into()),
        auth_username: None,
    });
    let publication = node.layer.publication(options)?;
    let operation = publication.start_publish(Body::text("state"), 600)?;
    let (initial, address) = request(&peer).await?;
    let mut challenge = node
        .endpoint
        .make_response(&initial, StatusCode::Unauthorized, None);
    challenge.headers.push(Header::WwwAuthenticate(
        "Digest realm=\"example\", nonce=\"test-nonce\", algorithm=MD5, qop=\"auth\"".into(),
    ));
    peer.send_to(challenge.to_string().as_bytes(), address)
        .await?;
    let (authenticated, address) = request(&peer).await?;
    assert_eq!(
        authenticated.cseq_header()?.seq()?,
        initial.cseq_header()?.seq()? + 1
    );
    assert!(value(&authenticated, "Authorization").is_some());
    let mut invalid = node
        .endpoint
        .make_response(&authenticated, StatusCode::OK, None);
    invalid.headers.push(Header::Expires(300.into()));
    peer.send_to(invalid.to_string().as_bytes(), address)
        .await?;
    let result = operation.wait().await;
    assert_eq!(result.attempts().len(), 2);
    assert_eq!(
        result.attempts()[0].response.as_ref().unwrap().status_code,
        StatusCode::Unauthorized
    );
    assert_eq!(result.response().unwrap().status_code, StatusCode::OK);
    assert!(result.signaling_result().is_err());
    assert!(publication.status().unknown_outcome);
    assert!(publication.status().etag.is_none());
    Ok(())
}

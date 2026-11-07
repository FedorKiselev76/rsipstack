#![cfg(feature = "platform-tokio")]
use rsipstack::{
    proxy::v2managed::{ProxyConfig, ProxyLayer, ProxyOperation, ProxyTarget},
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode, ToTypedHeader},
    transaction::{
        endpoint::{EndpointInnerRef, EndpointOption},
        EndpointBuilder,
    },
    transport::{udp::UdpConnection, TransportLayer},
};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(3);

struct Node {
    address: SocketAddr,
    endpoint: EndpointInnerRef,
    operations: mpsc::UnboundedReceiver<ProxyOperation>,
    stop: CancellationToken,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Node {
    async fn new(target: SocketAddr) -> Result<Self> {
        Self::with_timer_c(target, Duration::from_secs(5)).await
    }
    async fn with_timer_c(target: SocketAddr, timer_c: Duration) -> Result<Self> {
        let stop = CancellationToken::new();
        let transport = TransportLayer::new(stop.clone());
        let udp =
            UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone()))
                .await?;
        let address = udp.get_addr().get_socketaddr()?;
        transport.add_transport(udp.into());
        let endpoint = EndpointBuilder::new()
            .with_transport_layer(transport)
            .with_cancel_token(stop.clone())
            .with_option(EndpointOption {
                t1: Duration::from_millis(30),
                t2: Duration::from_millis(100),
                t4: Duration::from_millis(40),
                t1x64: Duration::from_millis(500),
                timerc: timer_c,
                // Explicit proxy mode must work on a UA-capable endpoint too.
                auto_ack_2xx: true,
                ..Default::default()
            })
            .build();
        let proxy = ProxyLayer::new(
            endpoint.inner.clone(),
            ProxyConfig::new(format!("sip:proxy@{address}").parse()?)?,
        );
        let mut incoming = endpoint.incoming_transactions()?;
        let (sender, operations) = mpsc::unbounded_channel();
        let router_stop = stop.clone();
        tokio::spawn(async move {
            loop {
                let tx = tokio::select! { _ = router_stop.cancelled() => break, tx = incoming.recv() => match tx { Some(tx) => tx, None => break } };
                let target = if tx
                    .original
                    .to_header()
                    .and_then(|h| h.tag())
                    .ok()
                    .flatten()
                    .is_none()
                {
                    ProxyTarget::next_hop(format!("sip:callee@{target}").parse().unwrap())
                } else {
                    ProxyTarget::routed()
                };
                let operation = proxy.forward(tx, target);
                let _ = sender.send(operation);
            }
        });
        let inner = endpoint.inner.clone();
        tokio::spawn(async move { endpoint.serve().await });
        Ok(Self {
            address,
            endpoint: inner,
            operations,
            stop,
        })
    }
    async fn operation(&mut self) -> Result<ProxyOperation> {
        Ok(tokio::time::timeout(DEADLINE, self.operations.recv())
            .await?
            .ok_or("no operation")?)
    }
}
async fn socket() -> Result<UdpSocket> {
    Ok(UdpSocket::bind("127.0.0.1:0").await?)
}
async fn receive(socket: &UdpSocket) -> Result<SipMessage> {
    let mut bytes = [0u8; 16384];
    let (length, _) = tokio::time::timeout(DEADLINE, socket.recv_from(&mut bytes)).await??;
    Ok(SipMessage::try_from(std::str::from_utf8(
        &bytes[..length],
    )?)?)
}
async fn request(socket: &UdpSocket) -> Result<Request> {
    match receive(socket).await? {
        SipMessage::Request(r) => Ok(r),
        other => Err(format!("expected request: {other}").into()),
    }
}
async fn response(socket: &UdpSocket, code: u16) -> Result<Response> {
    loop {
        match receive(socket).await? {
            SipMessage::Response(r) if r.status_code.code() == code => return Ok(r),
            SipMessage::Response(r) if r.status_code.code() == 100 => {}
            other => return Err(format!("expected {code}: {other}").into()),
        }
    }
}
fn invite(caller: SocketAddr) -> Result<Request> {
    Ok(Request::try_from(format!("INVITE sip:callee@destination.invalid SIP/2.0\r\nVia: SIP/2.0/UDP {caller};branch=z9hG4bKproxy-test;rport\r\nFrom: <sip:caller@example.test>;tag=caller\r\nTo: <sip:callee@example.test>\r\nCall-ID: transparent-proxy\r\nCSeq: 7 INVITE\r\nContact: <sip:caller@{caller}>\r\nMax-Forwards: 70\r\nX-End-To-End: preserved\r\nContent-Type: application/sdp\r\nContent-Length: 5\r\n\r\noffer").as_str())?)
}
async fn send(socket: &UdpSocket, addr: SocketAddr, message: impl ToString) -> Result {
    socket.send_to(message.to_string().as_bytes(), addr).await?;
    Ok(())
}
fn reply(
    node: &Node,
    request: &Request,
    code: u16,
    tag: &str,
    target: SocketAddr,
) -> Result<Response> {
    let mut r = node.endpoint.make_response(request, code.into(), None);
    r.to_header_mut()?.mut_tag(tag.into())?;
    r.headers
        .unique_push(Header::Contact(format!("<sip:callee@{target}>").into()));
    Ok(r)
}
fn ack(original: &Request, node: &Node, peer: SocketAddr, tag: &str) -> Result<Request> {
    let mut r = original.clone();
    r.method = Method::Ack;
    r.uri = format!("sip:callee@{peer}").parse()?;
    r.cseq_header_mut()?.mut_method(Method::Ack)?;
    r.to_header_mut()?.mut_tag(tag.into())?;
    r.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bKack-{tag};rport",
            original.via_header()?.typed()?.uri.host_with_port
        )
        .into(),
    ));
    r.headers.push_front(Header::Route(
        format!("<sip:proxy@{};lr>", node.address).into(),
    ));
    r.headers.retain(|h| !matches!(h, Header::ContentType(_)));
    r.headers.unique_push(Header::ContentLength(0.into()));
    r.body.clear();
    Ok(r)
}

#[tokio::test]
async fn identity_forked_successes_and_repeated_acks_stay_end_to_end() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    let original = invite(caller.local_addr()?)?;
    send(&caller, node.address, &original).await?;
    let operation = node.operation().await?;
    let forwarded = request(&peer).await?;
    assert_eq!(forwarded.uri, original.uri);
    assert_eq!(forwarded.call_id_header()?, original.call_id_header()?);
    assert_eq!(forwarded.from_header()?, original.from_header()?);
    assert_eq!(forwarded.to_header()?, original.to_header()?);
    assert_eq!(forwarded.cseq_header()?, original.cseq_header()?);
    assert_eq!(forwarded.contact_header()?, original.contact_header()?);
    assert_eq!(forwarded.body, original.body);
    assert_eq!(forwarded.max_forwards_header()?.value(), "69");
    assert!(forwarded
        .record_route_header()
        .ok_or("missing Record-Route")?
        .value()
        .contains(";lr"));
    let mut first = reply(&node, &forwarded, 200, "branch-one", peer.local_addr()?)?;
    let vias = first
        .headers
        .iter()
        .filter_map(|h| {
            if let Header::Via(v) = h {
                Some(v.value().to_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    first.headers.retain(|h| !matches!(h, Header::Via(_)));
    first.headers.push_front(Header::Via(vias.into()));
    send(&peer, node.address, &first).await?;
    let received = response(&caller, 200).await?;
    assert!(!received.has_multiple_vias());
    assert_eq!(received.contact_header()?, first.contact_header()?);
    assert!(
        tokio::time::timeout(Duration::from_millis(70), receive(&peer))
            .await
            .is_err(),
        "proxy must not ACK 2xx"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(70), receive(&caller))
            .await
            .is_err(),
        "proxy must not invent 2xx retransmissions"
    );
    for tag in ["branch-one", "branch-two", "branch-one"] {
        if tag != "branch-one" || operation.progress().responses_forwarded > 1 {
            let r = reply(&node, &forwarded, 200, tag, peer.local_addr()?)?;
            send(&peer, node.address, r).await?;
            assert_eq!(
                response(&caller, 200)
                    .await?
                    .to_header()?
                    .tag()?
                    .unwrap()
                    .to_string(),
                tag
            );
        }
        let ack = ack(&original, &node, peer.local_addr()?, tag)?;
        send(&caller, node.address, &ack).await?;
        let forwarded_ack = request(&peer).await?;
        assert_eq!(forwarded_ack.method, Method::Ack);
        assert_eq!(forwarded_ack.to_header()?, ack.to_header()?);
        assert!(forwarded_ack
            .headers
            .iter()
            .all(|h| !matches!(h, Header::Route(_))));
    }
    assert_eq!(
        operation.wait_final_response().await?.unwrap().status_code,
        StatusCode::OK
    );
    tokio::time::timeout(DEADLINE, operation.wait()).await??;
    assert_eq!(node.endpoint.get_stats().running_transactions, 0);
    Ok(())
}

#[tokio::test]
async fn cancel_waits_for_provisional_and_negative_ack_is_hop_by_hop() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    let original = invite(caller.local_addr()?)?;
    send(&caller, node.address, &original).await?;
    let operation = node.operation().await?;
    let forwarded = request(&peer).await?;
    let mut cancel = original.clone();
    cancel.headers.push(Header::Reason(
        "SIP;cause=487;text=\"caller cancelled\"".into(),
    ));
    cancel.method = Method::Cancel;
    cancel.cseq_header_mut()?.mut_method(Method::Cancel)?;
    cancel.body.clear();
    cancel.headers.unique_push(Header::ContentLength(0.into()));
    send(&caller, node.address, cancel).await?;
    assert_eq!(
        response(&caller, 200).await?.cseq_header()?.method()?,
        Method::Cancel
    );
    // INVITE retransmissions may occur before provisional; CANCEL must not.
    let before = tokio::time::timeout(Duration::from_millis(40), request(&peer)).await;
    if let Ok(message) = before {
        assert_eq!(message?.method, Method::Invite);
    }
    send(
        &peer,
        node.address,
        node.endpoint
            .make_response(&forwarded, StatusCode::Trying, None),
    )
    .await?;
    let forwarded_cancel = loop {
        let r = request(&peer).await?;
        if r.method == Method::Cancel {
            break r;
        }
        assert_eq!(r.method, Method::Invite);
    };
    assert_eq!(forwarded_cancel.via_header()?, forwarded.via_header()?);
    assert!(!forwarded_cancel.has_multiple_vias());
    assert!(forwarded_cancel
        .headers
        .iter()
        .any(|h| matches!(h, Header::Reason(r) if r.value().contains("caller cancelled"))));
    assert_eq!(forwarded_cancel.cseq_header()?.seq()?, 7);
    send(
        &peer,
        node.address,
        reply(&node, &forwarded_cancel, 200, "callee", peer.local_addr()?)?,
    )
    .await?;
    send(
        &peer,
        node.address,
        reply(&node, &forwarded, 487, "callee", peer.local_addr()?)?,
    )
    .await?;
    assert_eq!(request(&peer).await?.method, Method::Ack);
    response(&caller, 487).await?;
    let mut ack = original.clone();
    ack.method = Method::Ack;
    ack.cseq_header_mut()?.mut_method(Method::Ack)?;
    ack.to_header_mut()?.mut_tag("callee".into())?;
    send(&caller, node.address, ack).await?;
    let result = tokio::time::timeout(DEADLINE, operation.wait()).await??;
    assert!(result.cancel_submitted);
    assert!(
        tokio::time::timeout(Duration::from_millis(70), receive(&peer))
            .await
            .is_err()
    );
    assert_eq!(node.endpoint.get_stats().running_transactions, 0);
    Ok(())
}

#[tokio::test]
async fn routed_methods_and_challenges_are_forwarded_without_ua_policy() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    for (index, method) in [
        Method::Register,
        Method::Options,
        Method::Info,
        Method::Update,
        Method::PRack,
        Method::Invite,
        Method::Bye,
    ]
    .into_iter()
    .enumerate()
    {
        let mut original = invite(caller.local_addr()?)?;
        original.method = method.clone();
        original.cseq_header_mut()?.mut_method(method.clone())?;
        original
            .headers
            .unique_push(Header::CallId(format!("method-{index}").into()));
        original.uri = format!("sip:callee@{}", peer.local_addr()?).parse()?;
        if method != Method::Register {
            original.to_header_mut()?.mut_tag("callee".into())?;
            original.headers.push_front(Header::Route(
                format!("<sip:proxy@{};lr>", node.address).into(),
            ));
        }
        send(&caller, node.address, &original).await?;
        // Dropping observers must not stop the owned exchange.
        drop(node.operation().await?);
        let forwarded = request(&peer).await?;
        assert_eq!(forwarded.method, method);
        assert_eq!(forwarded.body, original.body);
        let mut final_response = reply(&node, &forwarded, 401, "callee", peer.local_addr()?)?;
        final_response.headers.push(Header::WwwAuthenticate(
            "Digest realm=\"peer\", nonce=\"abc\", algorithm=MD5".into(),
        ));
        send(&peer, node.address, &final_response).await?;
        assert_eq!(
            response(&caller, 401).await?.www_authenticate_header(),
            final_response.www_authenticate_header()
        );
        if method == Method::Invite {
            assert_eq!(request(&peer).await?.method, Method::Ack);
            let mut ack = original.clone();
            ack.method = Method::Ack;
            ack.cseq_header_mut()?.mut_method(Method::Ack)?;
            send(&caller, node.address, ack).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_validation_does_not_forward_exhausted_hops_or_proxy_require() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    for (index, header, code) in [
        (0, Header::MaxForwards("0".into()), 483),
        (1, Header::ProxyRequire("unknown-extension".into()), 420),
    ] {
        let mut original = invite(caller.local_addr()?)?;
        original
            .headers
            .unique_push(Header::CallId(format!("invalid-{index}").into()));
        original.headers.unique_push(Header::Via(
            format!(
                "SIP/2.0/UDP {};branch=z9hG4bKinvalid-{index};rport",
                caller.local_addr()?
            )
            .into(),
        ));
        original.headers.unique_push(header);
        send(&caller, node.address, &original).await?;
        drop(node.operation().await?);
        let response = response(&caller, code).await?;
        if code == 420 {
            assert!(response
                .headers
                .iter()
                .any(|h| matches!(h, Header::Unsupported(v) if v.value() == "unknown-extension")));
        }
        let mut ack = original.clone();
        ack.method = Method::Ack;
        ack.cseq_header_mut()?.mut_method(Method::Ack)?;
        ack.headers
            .unique_push(response.to_header()?.clone().into());
        send(&caller, node.address, ack).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(60), receive(&peer))
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn loose_and_strict_routes_choose_the_route_hop_without_losing_remote_target() -> Result {
    let caller = socket().await?;
    let default_peer = socket().await?;
    let route_hop = socket().await?;
    let mut node = Node::new(default_peer.local_addr()?).await?;
    for (index, loose) in [true, false].into_iter().enumerate() {
        let mut original = invite(caller.local_addr()?)?;
        original.method = Method::Options;
        original.cseq_header_mut()?.mut_method(Method::Options)?;
        original.headers.unique_push(Header::Via(
            format!(
                "SIP/2.0/UDP {};branch=z9hG4bKroute-{index};rport",
                caller.local_addr()?
            )
            .into(),
        ));
        original.headers.push_front(Header::Route(
            format!(
                "<sip:proxy@{};lr>, <sip:next@{}{}>",
                node.address,
                route_hop.local_addr()?,
                if loose { ";lr" } else { "" }
            )
            .into(),
        ));
        send(&caller, node.address, &original).await?;
        let operation = node.operation().await?;
        let forwarded = request(&route_hop).await?;
        let routes = forwarded
            .headers
            .iter()
            .filter_map(|h| {
                if let Header::Route(r) = h {
                    Some(r.value())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(routes.len(), 1);
        if loose {
            assert_eq!(forwarded.uri, original.uri);
            assert!(routes[0].contains(&route_hop.local_addr()?.to_string()));
        } else {
            assert_eq!(
                forwarded.uri.host_with_port.to_string(),
                route_hop.local_addr()?.to_string()
            );
            assert!(routes[0].contains(&original.uri.to_string()));
        }
        send(
            &route_hop,
            node.address,
            reply(&node, &forwarded, 200, "peer", route_hop.local_addr()?)?,
        )
        .await?;
        response(&caller, 200).await?;
        operation.wait().await?;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(60), receive(&default_peer))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn endpoint_shutdown_reports_interruption_and_releases_transactions() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    send(&caller, node.address, invite(caller.local_addr()?)?).await?;
    let operation = node.operation().await?;
    request(&peer).await?;
    node.stop.cancel();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, operation.wait()).await?,
        Err(rsipstack::proxy::v2managed::ProxyError::DriverStopped)
    ));
    assert_eq!(node.endpoint.get_stats().running_transactions, 0);
    Ok(())
}

#[tokio::test]
async fn reliable_provisional_retransmissions_pass_without_local_prack() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    send(&caller, node.address, invite(caller.local_addr()?)?).await?;
    let operation = node.operation().await?;
    let forwarded = request(&peer).await?;
    let mut ringing = reply(&node, &forwarded, 183, "callee", peer.local_addr()?)?;
    ringing.headers.push(Header::Require("100rel".into()));
    ringing.headers.push(Header::RSeq("10".into()));
    for _ in 0..2 {
        send(&peer, node.address, &ringing).await?;
        let received = response(&caller, 183).await?;
        assert_eq!(received.rseq_value(), Some(10));
        assert_eq!(received.body, ringing.body);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(60), receive(&peer))
            .await
            .is_err()
    );
    node.stop.cancel();
    assert!(operation.wait().await.is_err());
    Ok(())
}

#[tokio::test]
async fn sips_cannot_be_forwarded_to_a_plain_next_hop() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let mut node = Node::new(peer.local_addr()?).await?;
    let mut original = invite(caller.local_addr()?)?;
    original.uri = "sips:callee@secure.example".parse()?;
    send(&caller, node.address, &original).await?;
    let operation = node.operation().await?;
    response(&caller, 403).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(60), receive(&peer))
            .await
            .is_err()
    );
    node.stop.cancel();
    assert!(operation.wait().await.is_err());
    Ok(())
}

#[tokio::test]
async fn repeated_ack_is_dispatched_while_previous_ack_is_still_owned() -> Result {
    let caller = socket().await?;
    let peer = socket().await?;
    let stop = CancellationToken::new();
    let transport = TransportLayer::new(stop.clone());
    let udp =
        UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone())).await?;
    let address = udp.get_addr().get_socketaddr()?;
    transport.add_transport(udp.into());
    let endpoint = EndpointBuilder::new()
        .with_transport_layer(transport)
        .with_cancel_token(stop.clone())
        .build();
    let layer = ProxyLayer::new(
        endpoint.inner.clone(),
        ProxyConfig::new(format!("sip:proxy@{address}").parse()?)?,
    );
    let mut incoming = endpoint.incoming_transactions()?;
    let owner = tokio::spawn(async move { endpoint.serve().await });
    let mut ack = invite(caller.local_addr()?)?;
    ack.method = Method::Ack;
    ack.uri = format!("sip:peer@{}", peer.local_addr()?).parse()?;
    ack.cseq_header_mut()?.mut_method(Method::Ack)?;
    ack.to_header_mut()?.mut_tag("callee".into())?;
    send(&caller, address, &ack).await?;
    let first = tokio::time::timeout(DEADLINE, incoming.recv())
        .await?
        .ok_or("first ACK")?;
    send(&caller, address, &ack).await?;
    let second = tokio::time::timeout(DEADLINE, incoming.recv())
        .await?
        .ok_or("second ACK")?;
    let first = layer.forward(first, ProxyTarget::routed());
    let second = layer.forward(second, ProxyTarget::routed());
    for _ in 0..2 {
        let forwarded = request(&peer).await?;
        assert_eq!(forwarded.method, Method::Ack);
        assert_eq!(forwarded.body, ack.body);
    }
    first.wait().await?;
    second.wait().await?;
    stop.cancel();
    owner.await?;
    Ok(())
}

#[tokio::test]
async fn timer_c_refreshes_on_progress_then_cancels_without_inventing_a_peer_ack() -> Result {
    for answer_cancel in [true, false] {
        let caller = socket().await?;
        let peer = socket().await?;
        let mut node = Node::with_timer_c(peer.local_addr()?, Duration::from_millis(300)).await?;
        let original = invite(caller.local_addr()?)?;
        send(&caller, node.address, &original).await?;
        let operation = node.operation().await?;
        let forwarded = request(&peer).await?;
        send(
            &peer,
            node.address,
            reply(&node, &forwarded, 180, "callee", peer.local_addr()?)?,
        )
        .await?;
        response(&caller, 180).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        send(
            &peer,
            node.address,
            reply(&node, &forwarded, 183, "callee", peer.local_addr()?)?,
        )
        .await?;
        response(&caller, 183).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), receive(&peer))
                .await
                .is_err(),
            "Timer C must restart on progress"
        );
        let cancel = request(&peer).await?;
        assert_eq!(cancel.method, Method::Cancel);
        assert_eq!(cancel.via_header()?, forwarded.via_header()?);
        if answer_cancel {
            send(
                &peer,
                node.address,
                reply(&node, &cancel, 200, "callee", peer.local_addr()?)?,
            )
            .await?;
            send(
                &peer,
                node.address,
                reply(&node, &forwarded, 487, "callee", peer.local_addr()?)?,
            )
            .await?;
            assert_eq!(request(&peer).await?.method, Method::Ack);
        }
        let final_response = response(&caller, if answer_cancel { 487 } else { 408 }).await?;
        assert_eq!(
            final_response.to_header()?.tag()?.unwrap().to_string(),
            "callee"
        );
        let mut ack = original.clone();
        ack.method = Method::Ack;
        ack.cseq_header_mut()?.mut_method(Method::Ack)?;
        ack.headers
            .unique_push(final_response.to_header()?.clone().into());
        send(&caller, node.address, ack).await?;
        let result = tokio::time::timeout(DEADLINE, operation.wait()).await??;
        assert!(result.cancel_submitted);
        if !answer_cancel {
            // CANCEL may retransmit, but no peer INVITE final response existed
            // for an ACK: the 408 was generated locally by the proxy.
            while let Ok(message) =
                tokio::time::timeout(Duration::from_millis(70), request(&peer)).await
            {
                assert_eq!(message?.method, Method::Cancel);
            }
        }
    }
    Ok(())
}

#![cfg(feature = "platform-tokio")]
#![deny(deprecated)]
use rsipstack::{
    dialog::v2managed::{
        subscription_event_channel, AdmissionError, Body, DialogLayer, NotifierOptions,
        NotifyState, SubscriptionEventType, SubscriptionLayer, SubscriptionOptions,
        SubscriptionPhase,
    },
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
    fn options(&self, target: SocketAddr) -> Result<SubscriptionOptions> {
        Ok(SubscriptionOptions::new(
            format!("sip:alice@{}", self.address).try_into()?,
            format!("sip:presence@{target}").try_into()?,
            format!("sip:alice@{}", self.address).try_into()?,
            SubscriptionEventType::new("presence")?,
            60,
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
async fn recv(node: &mut Node) -> Result<Transaction> {
    Ok(timeout(Duration::from_secs(2), node.incoming.recv())
        .await?
        .ok_or("missing transaction")?)
}
async fn notify(
    node: &mut Node,
    layer: &SubscriptionLayer,
) -> Result<rsipstack::dialog::v2managed::SubscriptionDialog> {
    let incoming = layer.receive_notify(recv(node).await?)?;
    let dialog = incoming.dialog().clone();
    incoming
        .start_response(StatusCode::OK, None)?
        .wait_sent()
        .await?;
    tokio::task::yield_now().await;
    Ok(dialog)
}
fn notifier(node: &Node) -> Result<NotifierOptions> {
    Ok(NotifierOptions::new(
        format!("sip:notifier@{}", node.address).try_into()?,
        60,
    ))
}
async fn success(operation: &rsipstack::dialog::v2managed::ExchangeOperation) -> Result {
    let result = timeout(Duration::from_secs(2), operation.wait()).await?;
    result.signaling_result().map_err(Clone::clone)?;
    assert_eq!(
        result.response().ok_or("missing response")?.status_code,
        StatusCode::OK
    );
    Ok(())
}
#[tokio::test]
async fn subscribe_refresh_and_unsubscribe_own_responses_and_required_notifications() -> Result {
    let mut client = Node::new().await?;
    let mut server = Node::new().await?;
    let clients = client.layer.subscriptions();
    let servers = server.layer.subscriptions();
    let (ce, _) = subscription_event_channel();
    let (se, _) = subscription_event_channel();
    let (attempt, initial) = clients.start_subscription(client.options(server.address)?, ce)?;
    let tx = recv(&mut server).await?;
    let incoming = servers.receive_subscribe(tx, notifier(&server)?, se.clone())?;
    assert!(incoming
        .dialog()
        .start_notify(NotifyState::Active, None)
        .is_err());
    assert!(incoming
        .start_accept(61, NotifyState::Active, None)
        .is_err());
    let server_dialog = incoming.dialog().clone();
    let accepted = incoming.start_accept(
        60,
        NotifyState::Active,
        Some(Body::new("application/pidf+xml", "<presence/>")),
    )?;
    accepted.response.wait_sent().await?;
    success(&initial).await?;
    let dialog = notify(&mut client, &clients).await?;
    success(&accepted.notification).await?;
    assert_eq!(dialog.status().phase, SubscriptionPhase::Active);
    assert_eq!(attempt.branches().len(), 1);
    assert_eq!(
        dialog.status().id.local_tag,
        server_dialog.status().id.remote_tag
    );
    let refresh = dialog.start_refresh(30, None)?;
    drop(refresh.clone()); // observers do not own the transaction
    let incoming =
        servers.receive_subscribe(recv(&mut server).await?, notifier(&server)?, se.clone())?;
    assert_eq!(incoming.dialog().id(), server_dialog.id());
    let accepted = incoming.start_accept(25, NotifyState::Active, None)?;
    notify(&mut client, &clients).await?;
    success(&refresh).await?;
    success(&accepted.notification).await?;
    clients.close_admission();
    assert!(matches!(
        dialog.start_refresh(30, None),
        Err(AdmissionError::LayerClosed)
    ));
    let unsubscribe = dialog.start_unsubscribe()?;
    let incoming = servers.receive_subscribe(recv(&mut server).await?, notifier(&server)?, se)?;
    assert_eq!(incoming.requested_expires(), 0);
    let accepted = incoming.start_accept(0, NotifyState::Active, None)?;
    success(&unsubscribe).await?;
    assert!(!matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    let final_notify = clients.receive_notify(recv(&mut client).await?)?;
    assert_eq!(
        value(final_notify.request(), "Subscription-State").as_deref(),
        Some("terminated;reason=timeout")
    );
    final_notify
        .start_response(StatusCode::OK, None)?
        .wait_sent()
        .await?;
    success(&accepted.notification).await?;
    assert!(matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    assert!(clients.dialogs().is_empty());
    assert!(servers.dialogs().is_empty());
    assert!(clients.drain(Duration::from_secs(1)).await.is_drained());
    assert!(servers.drain(Duration::from_secs(1)).await.is_drained());
    Ok(())
}
async fn send_notify(
    peer: &UdpSocket,
    subscribe: &Request,
    local: SocketAddr,
    tag: &str,
    seq: u32,
    event: &str,
    state: &str,
) -> Result {
    let packet = format!("NOTIFY sip:alice@{local} SIP/2.0\r\nVia: SIP/2.0/UDP {};branch=z9hG4bK-{tag}-{seq}-{event}\r\nFrom: {};tag={tag}\r\nTo: {}\r\nCall-ID: {}\r\nCSeq: {seq} NOTIFY\r\nContact: <sip:notifier@{}>\r\nEvent: {event}\r\nSubscription-State: {state}\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", peer.local_addr()?, subscribe.to_header()?.value(), subscribe.from_header()?.value(), subscribe.call_id_header()?.value(), peer.local_addr()?);
    peer.send_to(packet.as_bytes(), local).await?;
    Ok(())
}
async fn response(peer: &UdpSocket) -> Result<rsipstack::sip::Response> {
    let mut bytes = [0; 8192];
    loop {
        let (n, _) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes)).await??;
        if let SipMessage::Response(response) = SipMessage::try_from(&bytes[..n])? {
            return Ok(response);
        }
    }
}
async fn answer(
    node: &Node,
    peer: &UdpSocket,
    request: &Request,
    local: SocketAddr,
    code: StatusCode,
    expires: u32,
) -> Result {
    let mut response = node.endpoint.make_response(request, code, None);
    if response.to_header()?.tag()?.is_none() {
        response
            .to_header_mut()?
            .mut_tag("response-only-tag".into())?;
    }
    response.headers.push(Header::Expires(expires.into()));
    response.headers.push(Header::Contact(
        format!("<sip:notifier@{}>", peer.local_addr()?).into(),
    ));
    response
        .headers
        .push(Header::RecordRoute("<sip:invalid.example;lr>".into()));
    peer.send_to(response.to_string().as_bytes(), local).await?;
    Ok(())
}
#[tokio::test]
async fn early_notify_forks_event_matching_and_cseq_are_isolated() -> Result {
    let mut client = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let layer = client.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let (attempt, initial) =
        layer.start_subscription(client.options(peer.local_addr()?)?, events)?;
    let (subscribe, local) = request(&peer).await?;
    send_notify(
        &peer,
        &subscribe,
        local,
        "fork-a",
        10,
        "presence",
        "active;expires=60",
    )
    .await?;
    let a = notify(&mut client, &layer).await?;
    assert_eq!(response(&peer).await?.status_code, StatusCode::OK);
    assert!(!initial.is_finished());
    send_notify(
        &peer,
        &subscribe,
        local,
        "fork-b",
        1,
        "presence",
        "pending;expires=45",
    )
    .await?;
    let b = notify(&mut client, &layer).await?;
    response(&peer).await?;
    assert_ne!(a.id().remote_tag, b.id().remote_tag);
    assert_eq!(attempt.branches().len(), 2);
    send_notify(
        &peer,
        &subscribe,
        local,
        "fork-a",
        90,
        "message-summary",
        "active;expires=60",
    )
    .await?;
    let rejection = match layer.receive_notify(recv(&mut client).await?) {
        Err(e) => e,
        Ok(_) => return Err("wrong Event was accepted".into()),
    };
    assert_eq!(rejection.status.code(), 481);
    rejection
        .transaction
        .start_final_response(rejection.status, None, None)?
        .wait_sent()
        .await?;
    response(&peer).await?;
    assert_eq!(a.status().remote_cseq, Some(10));
    send_notify(
        &peer,
        &subscribe,
        local,
        "fork-a",
        9,
        "presence",
        "active;expires=60",
    )
    .await?;
    let rejection = match layer.receive_notify(recv(&mut client).await?) {
        Err(e) => e,
        Ok(_) => return Err("stale CSeq was accepted".into()),
    };
    assert_eq!(rejection.status.code(), 500);
    rejection
        .transaction
        .start_final_response(rejection.status, None, None)?
        .wait_sent()
        .await?;
    response(&peer).await?;
    answer(&client, &peer, &subscribe, local, StatusCode::OK, 60).await?;
    success(&initial).await?;
    let refresh = a.start_refresh(30, None)?;
    let (request, local) = request(&peer).await?;
    assert_eq!(request.to_header()?.tag()?.unwrap().value(), "fork-a");
    assert!(request.route_headers().is_empty()); // route set came from NOTIFY, not SUBSCRIBE 2xx
    assert_eq!(
        request.cseq_header()?.seq()?,
        subscribe.cseq_header()?.seq()? + 1
    );
    answer(
        &client,
        &peer,
        &request,
        local,
        StatusCode::CallTransactionDoesNotExist,
        0,
    )
    .await?;
    let result = refresh.wait().await;
    result.signaling_result().map_err(Clone::clone)?;
    assert!(matches!(a.status().phase, SubscriptionPhase::Terminated(_)));
    assert_eq!(b.status().phase, SubscriptionPhase::Pending);
    Ok(())
}
#[tokio::test]
async fn missing_notify_times_out_and_dropped_notifier_responder_replies_501() -> Result {
    let client = Node::new().await?;
    let mut server = Node::new().await?;
    let layer = client.layer.subscriptions();
    let (events, mut notifications) = subscription_event_channel();
    let (attempt, initial) = layer.start_subscription(client.options(server.address)?, events)?;
    let tx = recv(&mut server).await?;
    let servers = server.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let incoming = servers.receive_subscribe(tx, notifier(&server)?, events)?;
    drop(incoming);
    assert_eq!(
        initial
            .wait()
            .await
            .response()
            .ok_or("response missing")?
            .status_code,
        StatusCode::NotImplemented
    );
    assert!(servers.drain(Duration::from_secs(1)).await.is_drained());
    assert!(servers.dialogs().is_empty());
    // A successful transaction is insufficient: Timer N requires NOTIFY.
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (events, mut timeout_events) = subscription_event_channel();
    let (pending, op) = layer.start_subscription(client.options(peer.local_addr()?)?, events)?;
    let (req, local) = request(&peer).await?;
    answer(&client, &peer, &req, local, StatusCode::OK, 60).await?;
    success(&op).await?;
    let event = timeout(Duration::from_secs(2), timeout_events.recv())
        .await?
        .ok_or("no Timer N event")?;
    assert!(matches!(
        event,
        rsipstack::dialog::v2managed::SubscriptionEvent::EstablishmentTimedOut { .. }
    ));
    assert!(!pending.waiting_notify());
    drop((attempt, notifications.recv()));
    Ok(())
}

#[tokio::test]
async fn notifier_expiry_sends_terminal_notify_and_reclaims_usage() -> Result {
    let mut client = Node::new().await?;
    let mut server = Node::new().await?;
    let clients = client.layer.subscriptions();
    let servers = server.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let mut options = client.options(server.address)?;
    options.expires = 1;
    let (_, initial) = clients.start_subscription(options, events.clone())?;
    let incoming =
        servers.receive_subscribe(recv(&mut server).await?, notifier(&server)?, events)?;
    let accepted = incoming.start_accept(1, NotifyState::Pending, None)?;
    let dialog = notify(&mut client, &clients).await?;
    success(&initial).await?;
    success(&accepted.notification).await?;
    let final_tx = recv(&mut client).await?;
    let final_request = final_tx.original.clone();
    let incoming = clients.receive_notify(final_tx)?;
    incoming
        .start_response(StatusCode::OK, None)?
        .wait_sent()
        .await?;
    assert_eq!(
        value(&final_request, "Subscription-State").as_deref(),
        Some("terminated;reason=timeout")
    );
    assert!(servers.drain(Duration::from_secs(1)).await.is_drained());
    assert!(clients.drain(Duration::from_secs(1)).await.is_drained());
    assert!(matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    assert!(servers.dialogs().is_empty());
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(clients.diagnostics().initial_attempts, 0);
    assert_eq!(clients.diagnostics().retired_responders, 0);
    Ok(())
}

#[tokio::test]
async fn refresh_without_notify_keeps_drain_pending_until_timer_n() -> Result {
    let mut client = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let layer = client.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let (_, initial) = layer.start_subscription(client.options(peer.local_addr()?)?, events)?;
    let (req, local) = request(&peer).await?;
    send_notify(
        &peer,
        &req,
        local,
        "peer",
        1,
        "presence",
        "active;expires=60",
    )
    .await?;
    let dialog = notify(&mut client, &layer).await?;
    response(&peer).await?;
    answer(&client, &peer, &req, local, StatusCode::OK, 60).await?;
    success(&initial).await?;
    let refresh = dialog.start_refresh(30, None)?;
    let (req, local) = request(&peer).await?;
    answer(&client, &peer, &req, local, StatusCode::OK, 30).await?;
    success(&refresh).await?;
    assert!(layer.drain(Duration::from_millis(20)).await.timed_out);
    assert!(layer.drain(Duration::from_secs(1)).await.is_drained());
    assert!(matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    assert!(layer.dialogs().is_empty());
    Ok(())
}

#[tokio::test]
async fn subscribe_auth_retry_preserves_attempts_and_timer_n() -> Result {
    let mut client = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let layer = client.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let mut options = client.options(peer.local_addr()?)?;
    options.credential = Some(rsipstack::dialog::v2managed::Credential {
        username: "alice".into(),
        password: "password".into(),
        realm: Some("example".into()),
        auth_username: None,
    });
    let (_, operation) = layer.start_subscription(options, events)?;
    let (initial, local) = request(&peer).await?;
    let mut challenge = client
        .endpoint
        .make_response(&initial, StatusCode::Unauthorized, None);
    challenge.headers.push(Header::WwwAuthenticate(
        "Digest realm=\"example\", nonce=\"test-nonce\", algorithm=MD5, qop=\"auth\"".into(),
    ));
    peer.send_to(challenge.to_string().as_bytes(), local)
        .await?;
    let (authenticated, local) = request(&peer).await?;
    assert!(value(&authenticated, "Authorization").is_some());
    assert_eq!(
        authenticated.cseq_header()?.seq()?,
        initial.cseq_header()?.seq()? + 1
    );
    send_notify(
        &peer,
        &authenticated,
        local,
        "notifier",
        1,
        "presence",
        "active;expires=60",
    )
    .await?;
    let dialog = notify(&mut client, &layer).await?;
    response(&peer).await?;
    answer(&client, &peer, &authenticated, local, StatusCode::OK, 60).await?;
    success(&operation).await?;
    assert_eq!(operation.wait().await.attempts().len(), 2);
    assert_eq!(
        dialog.status().local_cseq,
        authenticated.cseq_header()?.seq()?
    );
    assert!(layer.drain(Duration::from_secs(1)).await.is_drained());
    Ok(())
}

#[tokio::test]
async fn notify_route_set_preserves_strict_next_hop_and_allows_initial_zero_cseq() -> Result {
    let mut client = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let route = UdpSocket::bind("127.0.0.1:0").await?;
    let layer = client.layer.subscriptions();
    let (events, _) = subscription_event_channel();
    let (_, initial) = layer.start_subscription(client.options(peer.local_addr()?)?, events)?;
    let (subscribe, local) = request(&peer).await?;
    let packet = format!("NOTIFY sip:alice@{local} SIP/2.0\r\nVia: SIP/2.0/UDP {};branch=z9hG4bK-route-zero\r\nFrom: {};tag=routed\r\nTo: {}\r\nCall-ID: {}\r\nCSeq: 0 NOTIFY\r\nContact: <sip:notifier@{}>\r\nRecord-Route: <sip:strict@{}>\r\nEvent: presence\r\nSubscription-State: active;expires=60\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", peer.local_addr()?, subscribe.to_header()?.value(), subscribe.from_header()?.value(), subscribe.call_id_header()?.value(), peer.local_addr()?, route.local_addr()?);
    peer.send_to(packet.as_bytes(), local).await?;
    let dialog = notify(&mut client, &layer).await?;
    response(&peer).await?;
    assert_eq!(dialog.status().remote_cseq, Some(0));
    answer(&client, &peer, &subscribe, local, StatusCode::OK, 60).await?;
    success(&initial).await?;
    let refresh = dialog.start_refresh(30, None)?;
    let (routed, local) = request(&route).await?;
    assert_eq!(
        routed.uri.to_string(),
        format!("sip:strict@{}", route.local_addr()?)
    );
    assert_eq!(routed.route_headers().len(), 1);
    assert!(routed.route_headers()[0]
        .value()
        .contains(&peer.local_addr()?.to_string()));
    answer(
        &client,
        &route,
        &routed,
        local,
        StatusCode::CallTransactionDoesNotExist,
        0,
    )
    .await?;
    refresh
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    assert!(matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    Ok(())
}

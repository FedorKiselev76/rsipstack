//! Public, owned final-response API without dialog allocation.
#![cfg(feature = "platform-tokio")]
use rsipstack::{
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode},
    transaction::{
        endpoint::{EndpointInnerRef, EndpointOption},
        key::{TransactionKey, TransactionRole},
        response::ResponseError,
        EndpointBuilder, Transaction, TransactionReceiver,
    },
    transport::{channel::ChannelConnection, udp::UdpConnection, TransportEvent, TransportLayer},
};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const LIMIT: Duration = Duration::from_secs(2);
const SHORT: Duration = Duration::from_millis(25);

fn request(method: Method, address: SocketAddr) -> Result<Request> {
    Ok(format!("{method} sip:service@{address} SIP/2.0\r\nVia: SIP/2.0/UDP {address};branch=z9hG4bK-owned-response;rport\r\nFrom: <sip:peer@example.test>;tag=peer\r\nTo: <sip:service@example.test>\r\nCall-ID: owned-response\r\nCSeq: 7 {method}\r\nContent-Length: 0\r\n\r\n").as_str().try_into()?)
}
async fn receive(peer: &UdpSocket) -> Result<Response> {
    let mut bytes = [0; 8192];
    let (len, _) = tokio::time::timeout(LIMIT, peer.recv_from(&mut bytes)).await??;
    match std::str::from_utf8(&bytes[..len])?.try_into()? {
        SipMessage::Response(response) => Ok(response),
        _ => Err("expected response".into()),
    }
}
struct Node {
    address: SocketAddr,
    endpoint: EndpointInnerRef,
    incoming: TransactionReceiver,
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
        let udp =
            UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone()))
                .await?;
        let address = udp.get_addr().get_socketaddr()?;
        transport.add_transport(udp.into());
        let endpoint = EndpointBuilder::new()
            .with_transport_layer(transport)
            .with_cancel_token(stop.clone())
            .with_timer_interval(Duration::from_millis(5))
            .with_option(EndpointOption {
                t1: Duration::from_millis(40),
                t2: Duration::from_millis(80),
                t4: Duration::from_millis(20),
                t1x64: Duration::from_millis(400),
                ..Default::default()
            })
            .build();
        let incoming = endpoint.incoming_transactions()?;
        let inner = endpoint.inner.clone();
        tokio::spawn(async move { endpoint.serve().await });
        Ok(Self {
            address,
            endpoint: inner,
            incoming,
            stop,
        })
    }
    async fn accept(&mut self, peer: &UdpSocket, original: &Request) -> Result<Transaction> {
        peer.send_to(original.to_string().as_bytes(), self.address)
            .await?;
        Ok(tokio::time::timeout(LIMIT, self.incoming.recv())
            .await?
            .ok_or("no transaction")?)
    }
}

#[tokio::test]
async fn standalone_response_preserves_identity_body_and_cached_retransmission() -> Result {
    let mut node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let original = request(Method::Options, peer.local_addr()?)?;
    let tx = node.accept(&peer, &original).await?;
    // The old dialog path and new transaction path expose exactly one type.
    let receipt: rsipstack::dialog::response_operation::ResponseReceipt = tx.start_final_response(
        StatusCode::OK,
        Some(vec![Header::ContentType("text/plain".into())]),
        Some(b"ready".to_vec()),
    )?;
    let sent = tokio::time::timeout(LIMIT, receipt.wait_sent()).await??;
    assert!(sent.transport_accepted);
    assert!(!sent.checkpoint_committed && !sent.completion_committed);
    assert!(matches!(
        receipt.wait_ack().await,
        Err(ResponseError::AckNotApplicable)
    ));
    let response = receive(&peer).await?;
    assert_eq!(response.body, b"ready");
    assert_eq!(response.call_id_header()?, original.call_id_header()?);
    assert_eq!(response.from_header()?, original.from_header()?);
    assert_eq!(response.cseq_header()?, original.cseq_header()?);
    assert!(response.to_header()?.tag()?.is_some());
    assert!(response
        .headers
        .iter()
        .any(|h| matches!(h, Header::ContentLength(value) if value.value() == "5")));
    drop(receipt);
    peer.send_to(original.to_string().as_bytes(), node.address)
        .await?;
    assert_eq!(receive(&peer).await?, response);
    assert!(node.incoming.try_recv().is_err());
    assert_eq!(node.endpoint.transactions.len(), 0);
    Ok(())
}

#[tokio::test]
async fn rejected_invite_keeps_retransmitting_and_matches_ack_after_receipt_send() -> Result {
    let mut node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let original = request(Method::Invite, peer.local_addr()?)?;
    let tx = node.accept(&peer, &original).await?;
    let receipt = tx.start_final_response(StatusCode::BusyHere, None, None)?;
    tokio::time::timeout(LIMIT, receipt.wait_sent()).await??;
    let response = receive(&peer).await?;
    // Treat the first response as lost. The owning transaction sends it again.
    assert_eq!(receive(&peer).await?, response);
    let ack = node.endpoint.make_ack(&original, &response)?;
    let mut wrong = ack.clone();
    wrong.cseq_header_mut()?.mut_seq(8)?;
    peer.send_to(wrong.to_string().as_bytes(), node.address)
        .await?;
    assert!(tokio::time::timeout(SHORT, receipt.wait_ack())
        .await
        .is_err());
    peer.send_to(ack.to_string().as_bytes(), node.address)
        .await?;
    assert_eq!(
        tokio::time::timeout(LIMIT, receipt.wait_ack())
            .await??
            .cseq_header()?
            .seq()?,
        7
    );
    tokio::time::timeout(LIMIT, async {
        while node.endpoint.transactions.len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn rejected_invite_reports_ack_timeout_without_changing_send_result() -> Result {
    let mut node = Node::new().await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let tx = node
        .accept(&peer, &request(Method::Invite, peer.local_addr()?)?)
        .await?;
    let receipt = tx.start_final_response(StatusCode::CallTransactionDoesNotExist, None, None)?;
    tokio::time::timeout(LIMIT, receipt.wait_sent()).await??;
    assert!(matches!(
        tokio::time::timeout(LIMIT, receipt.wait_ack()).await?,
        Err(ResponseError::AckTimeout)
    ));
    assert!(receipt.wait_sent().await?.transport_accepted);
    Ok(())
}

async fn bounded(
    method: Method,
) -> Result<(
    Transaction,
    mpsc::Receiver<TransportEvent>,
    CancellationToken,
)> {
    let stop = CancellationToken::new();
    let endpoint = EndpointBuilder::new()
        .with_cancel_token(stop.clone())
        .build()
        .inner;
    let address: SocketAddr = "127.0.0.1:5061".parse()?;
    let (_, incoming) = mpsc::unbounded_channel();
    let (outgoing, receiver) = mpsc::channel(1);
    let conn =
        ChannelConnection::create_connection_bounded(incoming, outgoing, address.into(), None)
            .await?;
    let original = request(method, "127.0.0.1:5061".parse()?)?;
    // Fill the outgoing buffer to suspend the next transport send.
    conn.send(
        endpoint
            .make_response(&original, StatusCode::Trying, None)
            .into(),
    )
    .await?;
    let key = TransactionKey::from_request(&original, TransactionRole::Server)?;
    Ok((
        Transaction::new_server(key, original, endpoint, Some(conn.into())),
        receiver,
        stop,
    ))
}

#[tokio::test]
async fn dropping_waiters_or_all_receipts_does_not_abort_a_blocked_send() -> Result {
    for drop_receipt in [false, true] {
        let (tx, mut output, stop) = bounded(Method::Options).await?;
        let receipt = tx.start_final_response(StatusCode::OK, None, None)?;
        assert!(tokio::time::timeout(SHORT, receipt.wait_sent())
            .await
            .is_err());
        assert!(!receipt.progress().transport_accepted);
        let observer = receipt.clone();
        let waiter = tokio::spawn(async move { observer.wait_sent().await });
        waiter.abort();
        let receipt = (!drop_receipt).then_some(receipt);
        output.recv().await.ok_or("no filler")?;
        let event = tokio::time::timeout(LIMIT, output.recv())
            .await?
            .ok_or("send aborted")?;
        assert!(
            matches!(event, TransportEvent::Incoming(SipMessage::Response(r), _, _) if r.status_code == StatusCode::OK)
        );
        if let Some(receipt) = receipt {
            assert!(receipt.wait_sent().await?.transport_accepted);
        }
        stop.cancel();
    }
    Ok(())
}

#[tokio::test]
async fn transport_failure_and_forced_shutdown_are_reported_separately() -> Result {
    let (tx, output, stop) = bounded(Method::Options).await?;
    drop(output);
    let receipt = tx.start_final_response(StatusCode::OK, None, None)?;
    assert!(matches!(
        tokio::time::timeout(LIMIT, receipt.wait_sent()).await?,
        Err(ResponseError::Transport(_))
    ));
    assert!(!receipt.progress().transport_accepted);
    stop.cancel();
    let (tx, mut output, stop) = bounded(Method::Options).await?;
    let receipt = tx.start_final_response(StatusCode::OK, None, None)?;
    assert!(tokio::time::timeout(SHORT, receipt.wait_sent())
        .await
        .is_err());
    stop.cancel();
    assert!(matches!(
        tokio::time::timeout(LIMIT, receipt.wait_sent()).await?,
        Err(ResponseError::DriverStopped)
    ));
    output.recv().await.ok_or("no filler")?;
    assert!(output.try_recv().is_err());
    assert!(!receipt.progress().transport_accepted);
    Ok(())
}

#[tokio::test]
async fn invalid_status_ack_and_protocol_header_overrides_are_rejected_before_send() -> Result {
    for (method, status, headers) in [
        (Method::Options, StatusCode::Trying, None),
        (Method::Invite, StatusCode::OK, None),
        (Method::Ack, StatusCode::CallTransactionDoesNotExist, None),
        (
            Method::Options,
            StatusCode::OK,
            Some(vec![Header::Other("v".into(), "bad".into())]),
        ),
        (
            Method::Options,
            StatusCode::OK,
            Some(vec![Header::Other("L".into(), "999".into())]),
        ),
        (
            Method::Options,
            StatusCode::OK,
            Some(vec![Header::To("<sip:other@example.test>".into())]),
        ),
    ] {
        let (tx, mut output, stop) = bounded(method).await?;
        assert!(matches!(
            tx.start_final_response(status, headers, None),
            Err(ResponseError::Protocol(_))
        ));
        output.recv().await.ok_or("no filler")?;
        assert!(output.try_recv().is_err());
        stop.cancel();
    }
    let (mut tx, mut output, stop) = bounded(Method::Options).await?;
    output.recv().await;
    tx.reply(StatusCode::OK).await?;
    assert!(matches!(
        tx.start_final_response(StatusCode::BadRequest, None, None),
        Err(ResponseError::FinalAlreadyAdmitted)
    ));
    stop.cancel();
    Ok(())
}

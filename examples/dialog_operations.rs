//! Run the local API experiment against a small loopback SIP peer:
//! `cargo run --example dialog_operations`
//!
//! This is a signaling demonstration, not an interoperability or media test.
//! Initial establishment and confirmed-dialog commands use the experimental
//! owned operation API.
//! Persistence, PRACK and session timers are disabled.

use rsipstack::{
    dialog::v2managed::{
        DialogLayer, DialogOperation, InviteOption, OperationCompletion, ReinviteOptions,
        ResponseOrigin, TerminationOptions,
    },
    sip::{Header, HeadersExt, Method, SipMessage, StatusCode},
    transaction::{endpoint::EndpointInnerRef, EndpointBuilder},
    transport::{udp::UdpConnection, TransportLayer},
};
use std::{sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SDP: &str = "v=0\r\no=demo 1 1 IN IP4 127.0.0.1\r\ns=API demo\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 4000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n";

struct StopOnDrop(CancellationToken);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn observe(label: &str, operation: DialogOperation) -> Result<OperationCompletion> {
    // This identity is available before CSeq allocation. A business job can
    // retain the handle; it does not need to manufacture a SIP request.
    println!("{label}: admitted operation {}", operation.id().sequence());
    let result = tokio::time::timeout(Duration::from_secs(3), operation.wait()).await?;
    for attempt in result.attempts() {
        println!(
            "  CSeq={} transport_accepted={} response={:?} origin={:?} ACK_sent={}",
            attempt.request().cseq_header()?.seq()?,
            attempt.transport_accepted(),
            attempt.response().map(|r| r.status_code.clone()),
            attempt.response_origin(),
            attempt.ack_sent(),
        );
    }
    result.signaling_result().map_err(Clone::clone)?;
    Ok(result)
}

#[tokio::main]
async fn main() -> Result {
    let stop = CancellationToken::new();
    let _guard = StopOnDrop(stop.clone());
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let target = format!("sip:peer@{}", peer.local_addr()?);
    let transport = TransportLayer::new(stop.clone());
    let udp =
        UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone())).await?;
    let local = udp.get_addr().get_socketaddr()?;
    transport.add_transport(udp.into());
    let endpoint = Arc::new(
        EndpointBuilder::new()
            .with_transport_layer(transport)
            .with_cancel_token(stop.clone())
            .build(),
    );
    let layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
    let serving = endpoint.clone();
    let server = tokio::spawn(async move { serving.serve().await });
    let peer_task = tokio::spawn(run_peer(peer, endpoint.inner.clone(), stop.clone()));
    let (states, _events) = layer.new_dialog_event_channel();

    let (dialog, initial) = layer.start_invite(
        InviteOption {
            caller: format!("sip:caller@{local}").try_into()?,
            callee: target.try_into()?,
            contact: format!("sip:caller@{local}").try_into()?,
            offer: Some(SDP.as_bytes().to_vec()),
            content_type: Some("application/sdp".into()),
            ..Default::default()
        },
        states,
    )?;
    let completed = observe("Initial INVITE", initial.operation()).await?;
    assert_eq!(
        completed.response().map(|r| r.status_code.clone()),
        Some(StatusCode::OK)
    );
    println!("Initial INVITE: established through an owned operation");

    let info = observe("INFO", dialog.start_request(Method::Info, None, None)?).await?;
    assert_eq!(
        info.response().map(|r| r.status_code.clone()),
        Some(StatusCode::OK)
    );
    assert_eq!(info.response_origin(), Some(ResponseOrigin::Peer));

    let negotiation = observe(
        "re-INVITE",
        dialog.start_reinvite(
            Some(vec![Header::ContentType("application/sdp".into())]),
            Some(SDP.as_bytes().to_vec()),
            ReinviteOptions::default(),
        )?,
    )
    .await?;
    assert_eq!(
        negotiation.response().map(|r| r.status_code.clone()),
        Some(StatusCode::NotAcceptableHere),
    );
    assert!(negotiation.dialog_state().is_confirmed());
    println!("re-INVITE: peer rejected the offer; signaling completed; call remains active");

    let termination = dialog.start_termination(TerminationOptions::default())?;
    let ended = tokio::time::timeout(Duration::from_secs(3), termination.wait()).await?;
    ended.signaling_result().map_err(Clone::clone)?;
    assert!(ended.dialog_state().is_terminated());
    // With persistence disabled this is immediate. It is not a transaction drain.
    dialog.persistence_barrier().wait().await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), peer_task).await???,
        2
    );
    println!("BYE: session terminated; peer observed both INVITE ACKs");

    layer.remove_dialog(&dialog.id());
    // End this bounded demo. Production shutdown must also account for retained
    // transactions and ACK caches; the operation result is not that guarantee.
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await??;
    Ok(())
}

async fn run_peer(
    peer: UdpSocket,
    endpoint: EndpointInnerRef,
    stop: CancellationToken,
) -> Result<usize> {
    let mut initial_cseq = None;
    let mut acks = 0;
    let mut bytes = [0; 8192];
    loop {
        let (len, source) = tokio::select! {
            _ = stop.cancelled() => return Err("demo stopped before BYE".into()),
            packet = peer.recv_from(&mut bytes) => packet?,
        };
        let SipMessage::Request(request) = std::str::from_utf8(&bytes[..len])?.try_into()? else {
            return Err("peer expected a SIP request".into());
        };
        if request.method == Method::Ack {
            acks += 1;
            continue;
        }
        let initial = request.method == Method::Invite
            && *initial_cseq.get_or_insert(request.cseq_header()?.seq()?)
                == request.cseq_header()?.seq()?;
        let status = match request.method {
            Method::Invite if !initial => StatusCode::NotAcceptableHere,
            Method::Invite | Method::Info | Method::Bye => StatusCode::OK,
            _ => return Err(format!("unexpected method: {}", request.method).into()),
        };
        let mut response = endpoint.make_response(&request, status, None);
        response.to_header_mut()?.mut_tag("demo-peer".into())?;
        if initial {
            response.headers.unique_push(Header::Contact(
                format!("<sip:peer@{}>", peer.local_addr()?).into(),
            ));
            response
                .headers
                .unique_push(Header::ContentType("application/sdp".into()));
            response.body = SDP.as_bytes().to_vec();
            response.headers.unique_push(Header::ContentLength(
                response.body.len().to_string().into(),
            ));
        }
        peer.send_to(response.to_string().as_bytes(), source)
            .await?;
        if request.method == Method::Bye {
            return Ok(acks);
        }
    }
}

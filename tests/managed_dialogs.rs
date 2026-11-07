//! Wire-level ownership through the isolated public facade, also run without legacy.
#![cfg(feature = "platform-tokio")]
#![deny(deprecated)]
use rsipstack::{
    dialog::v2managed::{
        dialog_layer::DialogLayer,
        events::{DialogEvent, DialogNotification},
        DialogCheckpoint, DialogCheckpointReason, DialogPersistence, DialogPersistenceConfig,
        DialogSnapshot, DialogSnapshotState, SessionTimerConfig,
    },
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode},
    transaction::{
        key::{TransactionKey, TransactionRole},
        EndpointBuilder, Transaction,
    },
    transport::{channel::ChannelConnection, udp::UdpConnection, TransportEvent, TransportLayer},
};
use std::{sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Stop(CancellationToken);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
async fn response(peer: &UdpSocket) -> Result<Response> {
    let mut bytes = [0; 8192];
    let (length, _) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes)).await??;
    match SipMessage::try_from(&bytes[..length])? {
        SipMessage::Response(response) => Ok(response),
        _ => Err("expected response".into()),
    }
}

#[tokio::test]
async fn managed_early_bye_finishes_the_owned_invite_and_retires_the_registry() -> Result {
    let stop = Stop(CancellationToken::new());
    let transport = TransportLayer::new(stop.0.clone());
    let udp = UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.0.clone()))
        .await?;
    let local = udp.get_addr().get_socketaddr()?;
    transport.add_transport(udp.into());
    let endpoint = EndpointBuilder::new()
        .with_transport_layer(transport)
        .with_cancel_token(stop.0.clone())
        .build();
    let endpoint_inner = endpoint.inner.clone();
    let layer = Arc::new(DialogLayer::new(endpoint_inner.clone()));
    let (states, mut events) = layer.new_dialog_event_channel();
    let (calls, mut incoming) = mpsc::unbounded_channel();
    let mut transactions = endpoint.incoming_transactions()?;
    let router = layer.clone();
    let routing_stop = stop.0.clone();
    tokio::spawn(async move {
        loop {
            let transaction = tokio::select! {
                _ = routing_stop.cancelled() => break,
                value = transactions.recv() => match value { Some(tx) => tx, None => break },
            };
            if let Some(dialog) = router.match_dialog(&transaction) {
                let _ = dialog.start_incoming_request(transaction).unwrap();
            } else {
                calls
                    .send(
                        router
                            .start_incoming_invite(
                                transaction,
                                states.clone(),
                                Some(format!("sip:local@{local}").parse().unwrap()),
                            )
                            .unwrap(),
                    )
                    .unwrap();
            }
        }
    });
    tokio::spawn(async move { endpoint.serve().await });
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let remote = peer.local_addr()?;
    let invite = Request::try_from(format!(
        "INVITE sip:local@{local} SIP/2.0\r\nVia: SIP/2.0/UDP {remote};branch=z9hG4bK-v2-invite;rport\r\nFrom: <sip:peer@{remote}>;tag=peer\r\nTo: <sip:local@{local}>\r\nCall-ID: v2-boundary\r\nCSeq: 1 INVITE\r\nContact: <sip:peer@{remote}>\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
    ).as_str())?;
    peer.send_to(invite.to_string().as_bytes(), local).await?;
    let incoming = timeout(Duration::from_secs(2), incoming.recv())
        .await?
        .ok_or("missing INVITE")?;
    let dialog = incoming.dialog();
    dialog
        .start_response(StatusCode::Ringing, None, None)?
        .wait_sent()
        .await?;
    let ringing = loop {
        let r = response(&peer).await?;
        if r.status_code == StatusCode::Ringing {
            break r;
        }
    };
    // Closing application admission must not stop a peer BYE or its final INVITE response.
    layer.close_admission();
    let mut bye = invite.clone();
    bye.method = Method::Bye;
    bye.headers.unique_push(Header::CSeq("2 BYE".into()));
    bye.headers
        .unique_push(Header::To(ringing.to_header()?.clone()));
    bye.headers.unique_push(Header::Via(
        format!("SIP/2.0/UDP {remote};branch=z9hG4bK-v2-bye;rport").into(),
    ));
    peer.send_to(bye.to_string().as_bytes(), local).await?;
    let mut bye_ok = false;
    let mut final_invite = None;
    while !bye_ok || final_invite.is_none() {
        let r = response(&peer).await?;
        match r.cseq_header()?.method()? {
            Method::Bye => {
                assert_eq!(r.status_code, StatusCode::OK);
                bye_ok = true;
            }
            Method::Invite if r.status_code == StatusCode::RequestTerminated => {
                final_invite = Some(r)
            }
            _ => {}
        }
    }
    let ack = endpoint_inner.make_ack(&invite, &final_invite.unwrap())?;
    peer.send_to(ack.to_string().as_bytes(), local).await?;
    timeout(Duration::from_secs(2), incoming.wait()).await??;
    assert!(dialog.state().is_terminated());
    timeout(Duration::from_secs(2), async {
        while let Some(event) = events.recv().await {
            if let DialogEvent::Notification(DialogNotification::Terminated(id, _)) = event {
                assert_eq!(id, dialog.id());
                return;
            }
        }
        panic!("missing terminal event");
    })
    .await?;
    layer.remove_dialog(&dialog.id());
    assert!(layer.is_empty());
    Ok(())
}

#[tokio::test]
async fn closed_layer_returns_the_unconsumed_initial_transaction() -> Result {
    let endpoint = EndpointBuilder::new().build();
    let layer = DialogLayer::new(endpoint.inner.clone());
    let (events, _receiver) = layer.new_dialog_event_channel();
    layer.close_admission();
    let request = Request::try_from("INVITE sip:local@127.0.0.1 SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1;branch=z9hG4bK-closed\r\nFrom: <sip:peer@127.0.0.1>;tag=peer\r\nTo: <sip:local@127.0.0.1>\r\nCall-ID: closed-layer\r\nCSeq: 1 INVITE\r\nContact: <sip:peer@127.0.0.1>\r\nContent-Length: 0\r\n\r\n")?;
    let key = TransactionKey::from_request(&request, TransactionRole::Server)?;
    let transaction = Transaction::new_server(key, request.clone(), endpoint.inner.clone(), None);
    let error = match layer.start_incoming_invite(transaction, events, None) {
        Err(error) => error,
        Ok(_) => panic!("closed layer admitted a new call"),
    };
    assert_eq!(error.transaction.unwrap().original, request);
    assert!(layer.is_empty());
    assert!(layer.drain(Duration::from_millis(20)).await.is_drained());
    Ok(())
}

struct TerminalSnapshots(mpsc::UnboundedSender<DialogSnapshot>);
#[async_trait::async_trait]
impl DialogPersistence for TerminalSnapshots {
    async fn save(&self, checkpoint: DialogCheckpoint) -> rsipstack::Result<()> {
        if checkpoint.reason == DialogCheckpointReason::Terminated {
            self.0.send(checkpoint.snapshot).unwrap();
        }
        Ok(())
    }
}

#[test]
fn session_timer_rejection_persists_the_terminated_dialog() -> Result {
    // An async timeout cannot fire if the runtime itself is blocked on a dialog mutex.
    let (done, result) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(check_timer_rejections());
    });
    result.recv_timeout(Duration::from_secs(5))?
}

#[tokio::main(flavor = "current_thread")]
async fn check_timer_rejections() -> Result {
    for (config, expected) in [(None, 420), (Some(SessionTimerConfig::default()), 422)] {
        let endpoint = EndpointBuilder::new().build();
        let layer = DialogLayer::new(endpoint.inner.clone());
        layer.set_session_timer(config)?;
        let (saved, mut snapshots) = mpsc::unbounded_channel();
        layer.set_persistence(Some(DialogPersistenceConfig::new(Arc::new(
            TerminalSnapshots(saved),
        ))))?;

        let (wire, mut responses) = mpsc::unbounded_channel();
        let remote = "127.0.0.1:5060".parse::<std::net::SocketAddr>()?;
        let conn = ChannelConnection::create_connection(
            mpsc::unbounded_channel().1,
            wire,
            remote.into(),
            None,
        )
        .await?;
        let request = Request::try_from(concat!(
            "INVITE sip:local@localhost SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-timer\r\n",
            "From: <sip:peer@localhost>;tag=peer\r\nTo: <sip:local@localhost>\r\nCall-ID: timer-reject\r\n",
            "CSeq: 1 INVITE\r\nContact: <sip:peer@localhost>\r\nRequire: timer\r\nSession-Expires: 60\r\nContent-Length: 0\r\n\r\n",
        ))?;
        let key = TransactionKey::from_request(&request, TransactionRole::Server)?;
        let tx = Transaction::new_server(key, request, endpoint.inner.clone(), Some(conn.into()));
        let (events, _receiver) = layer.new_dialog_event_channel();
        let incoming =
            layer.start_incoming_invite(tx, events, Some("sip:local@localhost".parse()?))?;
        incoming.wait().await?;
        let Some(TransportEvent::Incoming(SipMessage::Response(response), _, _)) =
            responses.recv().await
        else {
            return Err("expected INVITE rejection".into());
        };
        assert_eq!(response.status_code.code(), expected);
        let dialog = incoming.dialog();
        dialog.persistence_barrier().wait().await?;
        let snapshot = snapshots.try_recv()?;
        assert_eq!(snapshot.id, dialog.id());
        assert_eq!(snapshot.state, DialogSnapshotState::Terminated);
    }
    Ok(())
}

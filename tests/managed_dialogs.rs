//! Wire-level ownership through the isolated public facade, also run without legacy.
#![cfg(feature = "platform-tokio")]
#![deny(deprecated)]
use rsipstack::{
    dialog::v2managed::{
        dialog_layer::DialogLayer,
        events::{DialogEvent, DialogNotification},
    },
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode},
    transaction::EndpointBuilder,
    transport::{udp::UdpConnection, TransportLayer},
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
    use rsipstack::transaction::{
        key::{TransactionKey, TransactionRole},
        Transaction,
    };
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

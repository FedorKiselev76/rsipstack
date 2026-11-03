//! Wire regressions for the B2BUA integration API, independent of PRACK.
use super::test_in_dialog_provisional::{
    establish, establish_with_credential, recv_request, reply, response,
};
use crate::dialog::{
    dialog::DialogState,
    invite_dialog::{AckBody, InviteAnswerHandler},
};
use crate::sip::{Header, HeadersExt, Method, Response};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

const OFFER: &[u8] = b"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 10000 RTP/AVP 0\r\n";
const ANSWER: &[u8] = b"v=0\r\no=- 2 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 20000 RTP/AVP 0\r\n";
struct Answer {
    offers: mpsc::Sender<Response>,
    answers: Mutex<mpsc::Receiver<Vec<u8>>>,
}
#[async_trait::async_trait]
impl InviteAnswerHandler for Answer {
    async fn answer(&self, response: Response) -> crate::Result<AckBody> {
        self.offers.send(response).await.unwrap();
        Ok(AckBody::sdp(
            self.answers.lock().await.recv().await.unwrap(),
        ))
    }
}

#[tokio::test]
async fn deferred_ack_waits_for_answer_and_retransmits_identically() -> crate::Result<()> {
    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();
    let (dialog, _states, peer) = establish_with_credential(&token, Some(credential())).await?;
    let before = dialog.snapshot().local_cseq;
    let request = dialog.prepare_request(Method::Invite, None, None)?;
    assert_eq!(dialog.snapshot().local_cseq, before + 1);

    let (offers, mut received_offer) = mpsc::channel(1);
    let (answers, receive_answer) = mpsc::channel(1);
    let handler = Arc::new(Answer {
        offers,
        answers: Mutex::new(receive_answer),
    });
    let pending =
        tokio::spawn(async move { dialog.send_offerless_reinvite(request, handler).await });
    let (request, address) = recv_request(&peer, Method::Invite).await;
    challenge(&peer, address, &request).await?;
    recv_request(&peer, Method::Ack).await;
    let (request, address) = recv_request(&peer, Method::Invite).await;
    let sequence = request.cseq_header()?.seq()?;
    assert!(request.authorization_header().is_some());
    assert!(request.body.is_empty());
    let contact = format!("sip:peer@{}", peer.local_addr()?);
    let mut success = Response::try_from(response(&request, 200, "OK", &contact).as_str())?;
    success.body = OFFER.into();
    success
        .headers
        .unique_push(Header::ContentType("application/sdp".into()));
    success
        .headers
        .unique_push(Header::ContentLength((OFFER.len() as u32).into()));
    peer.send_to(success.to_string().as_bytes(), address)
        .await?;
    assert_eq!(received_offer.recv().await.unwrap().body, OFFER);
    let mut packet = [0; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut packet))
            .await
            .is_err(),
        "no automatic empty ACK while waiting for the answer"
    );
    answers.send(ANSWER.into()).await.unwrap();
    let (ack, _) = recv_request(&peer, Method::Ack).await;
    assert_eq!(ack.body, ANSWER);
    assert_eq!(ack.cseq_header()?.seq()?, sequence);
    assert_eq!(pending.await.unwrap()?.unwrap().status_code.code(), 200);
    peer.send_to(success.to_string().as_bytes(), address)
        .await?;
    let (repeated, _) = recv_request(&peer, Method::Ack).await;
    assert_eq!(repeated, ack);
    Ok(())
}

#[tokio::test]
async fn cancelled_reinvite_waits_for_provisional_and_preserves_session() -> crate::Result<()> {
    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();
    let (dialog, _states, peer) = establish_with_credential(&token, Some(credential())).await?;
    let request = dialog.prepare_request(Method::Invite, None, Some(OFFER.into()))?;
    let cancel = CancellationToken::new();
    let cancellation = cancel.clone();
    let sender = dialog.clone();
    let pending = tokio::spawn(async move {
        sender
            .send_cancellable_request(request, cancellation, None)
            .await
    });
    let (request, address) = recv_request(&peer, Method::Invite).await;
    challenge(&peer, address, &request).await?;
    recv_request(&peer, Method::Ack).await;
    let (request, address) = recv_request(&peer, Method::Invite).await;
    assert!(request.authorization_header().is_some());
    cancel.cancel();
    let mut packet = [0; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut packet))
            .await
            .is_err(),
        "CANCEL must wait for a provisional"
    );
    reply(&peer, address, &request, 100, "Trying").await;
    let (cancel, _) = recv_request(&peer, Method::Cancel).await;
    assert_eq!(cancel.via_header()?, request.via_header()?);
    assert_eq!(cancel.cseq_header()?.seq()?, request.cseq_header()?.seq()?);
    reply(&peer, address, &cancel, 200, "OK").await;
    reply(&peer, address, &request, 487, "Request Terminated").await;
    recv_request(&peer, Method::Ack).await;
    assert_eq!(pending.await.unwrap()?.unwrap().status_code.code(), 487);
    assert!(matches!(dialog.state(), DialogState::Confirmed(..)));
    Ok(())
}

#[tokio::test]
async fn sequential_early_branches_keep_identities_and_reject_a_stale_ack() -> crate::Result<()> {
    use super::test_accept_before_handle::incoming_invite;
    use crate::{dialog::dialog_layer::DialogLayer, transaction::transaction::TransactionEvent};
    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();
    let (_old, mut tx, _old_states, peer) = incoming_invite(&token).await?;
    tx.original.body = OFFER.into();
    tx.original
        .headers
        .unique_push(Header::ContentLength((OFFER.len() as u32).into()));
    let original = tx.original.clone();
    let input = tx.tu_sender.clone();
    let connection = tx.connection.clone();
    let layer = Arc::new(DialogLayer::new(tx.endpoint_inner.clone()));
    let (states, mut events) = mpsc::unbounded_channel();
    let session = layer.server_invite_session(tx, states, "sip:bridge@127.0.0.1".try_into()?)?;
    let first = session.branch("first-attempt")?;
    session.respond("first-attempt", 183.into(), None, Some(ANSWER.into()))?;
    let second = session.branch("second-attempt")?;
    session.respond("second-attempt", 183.into(), None, Some(ANSWER.into()))?;
    assert_ne!(first.id(), second.id());
    session.respond("second-attempt", 200.into(), None, Some(ANSWER.into()))?;
    assert!(layer.get_dialog(&first.id()).is_none());
    assert!(layer.get_dialog(&second.id()).is_some());
    let mut packet = [0; 4096];
    let (success, address) = loop {
        let (len, source) =
            tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut packet))
                .await
                .unwrap()?;
        if let crate::sip::SipMessage::Response(response) =
            crate::sip::SipMessage::try_from(&packet[..len])?
        {
            if response.status_code.code() == 200 {
                break (response, source);
            }
        }
    };
    let mut ack = original;
    ack.method = Method::Ack;
    ack.body.clear();
    ack.headers.unique_push(Header::CSeq("1 ACK".into()));
    ack.headers.unique_push(Header::ContentLength(0.into()));
    ack.headers.unique_push(Header::To(
        "<sip:bridge@127.0.0.1>;tag=first-attempt".into(),
    ));
    // Even if an old early-dialog handle delivered this into the shared
    // transaction, it must not stop the selected branch's ACK timer.
    input.send(TransactionEvent::Received(ack.clone().into(), connection))?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(second.state(), DialogState::WaitAck(..)));
    ack.headers
        .unique_push(Header::To(success.to_header()?.clone()));
    peer.send_to(ack.to_string().as_bytes(), address).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(event) = events.recv().await {
            if let DialogState::Confirmed(id, _) = event {
                assert_eq!(id, second.id());
                return;
            }
        }
        panic!("missing confirmation");
    })
    .await
    .unwrap();
    assert_eq!(
        second.last_remote_ack().unwrap().to_header()?,
        success.to_header()?
    );
    Ok(())
}

fn credential() -> crate::dialog::authenticate::Credential {
    crate::dialog::authenticate::Credential {
        username: "alice".into(),
        password: "secret".into(),
        ..Default::default()
    }
}

async fn challenge(
    peer: &tokio::net::UdpSocket,
    address: std::net::SocketAddr,
    req: &crate::sip::Request,
) -> crate::Result<()> {
    let mut resp = Response::try_from(
        response(
            req,
            401,
            "Unauthorized",
            &format!("sip:peer@{}", peer.local_addr()?),
        )
        .as_str(),
    )?;
    resp.headers.push(Header::WwwAuthenticate(
        r#"Digest realm="test", nonce="nonce", algorithm=MD5, qop="auth""#.into(),
    ));
    peer.send_to(resp.to_string().as_bytes(), address).await?;
    Ok(())
}

#[tokio::test]
async fn challenge_without_credentials_preserves_confirmed_dialog() -> crate::Result<()> {
    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();
    let (dialog, _states, peer) = establish(&token).await?;
    let sender = dialog.clone();
    let request = dialog.prepare_request(Method::Invite, None, Some(OFFER.into()))?;
    let pending = tokio::spawn(async move { sender.send_request(request).await });
    let (request, address) = recv_request(&peer, Method::Invite).await;
    challenge(&peer, address, &request).await?;
    recv_request(&peer, Method::Ack).await;
    assert_eq!(pending.await.unwrap()?.unwrap().status_code.code(), 401);
    assert!(matches!(dialog.state(), DialogState::Confirmed(..)));
    Ok(())
}

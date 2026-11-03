//! Wire regressions for application-controlled re-INVITEs on confirmed dialogs.
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
    let request = dialog.prepare_reinvite(None, None)?;
    assert_eq!(dialog.snapshot().local_cseq, before + 1);

    let (offers, mut received_offer) = mpsc::channel(1);
    let (answers, receive_answer) = mpsc::channel(1);
    let handler = Arc::new(Answer {
        offers,
        answers: Mutex::new(receive_answer),
    });
    let pending = tokio::spawn(async move {
        dialog
            .send_reinvite(request, CancellationToken::new(), Some(handler))
            .await
    });
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
    let request = dialog.prepare_reinvite(None, Some(OFFER.into()))?;
    let cancel = CancellationToken::new();
    let cancellation = cancel.clone();
    let sender = dialog.clone();
    let pending =
        tokio::spawn(async move { sender.send_reinvite(request, cancellation, None).await });
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
    let request = dialog.prepare_reinvite(None, Some(OFFER.into()))?;
    let pending = tokio::spawn(async move {
        sender
            .send_reinvite(request, CancellationToken::new(), None)
            .await
    });
    let (request, address) = recv_request(&peer, Method::Invite).await;
    challenge(&peer, address, &request).await?;
    recv_request(&peer, Method::Ack).await;
    assert_eq!(pending.await.unwrap()?.unwrap().status_code.code(), 401);
    assert!(matches!(dialog.state(), DialogState::Confirmed(..)));
    Ok(())
}

async fn receive_response(
    peer: &tokio::net::UdpSocket,
    method: Method,
    code: u16,
) -> crate::Result<(Response, std::net::SocketAddr)> {
    let mut bytes = [0; 8192];
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (len, address) = peer.recv_from(&mut bytes).await?;
            if let crate::sip::SipMessage::Response(response) =
                crate::sip::SipMessage::try_from(&bytes[..len])?
            {
                if response.cseq_header()?.method()? == method
                    && response.status_code.code() == code
                {
                    return Ok((response, address));
                }
            }
        }
    })
    .await
    .expect("response deadline")
}

#[tokio::test]
async fn server_leg_reinvite_requires_initial_ack_and_preserves_call_on_488() -> crate::Result<()> {
    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();
    let (dialog, mut tx, mut states, peer) =
        super::test_accept_before_handle::incoming_invite(&token).await?;
    let original = tx.original.clone();
    assert!(dialog.prepare_reinvite(None, Some(OFFER.into())).is_err());
    dialog.accept(None, None)?;
    let mut driver = dialog.clone();
    tokio::spawn(async move { driver.handle(&mut tx).await });
    let (success, address) = receive_response(&peer, Method::Invite, 200).await?;
    assert!(matches!(dialog.state(), DialogState::WaitAck(..)));
    assert!(dialog.prepare_reinvite(None, Some(OFFER.into())).is_err());
    let mut ack = original;
    ack.method = Method::Ack;
    ack.headers.unique_push(Header::CSeq("1 ACK".into()));
    ack.headers
        .unique_push(Header::To(success.to_header()?.clone()));
    peer.send_to(ack.to_string().as_bytes(), address).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(state) = states.recv().await {
            if matches!(state, DialogState::Confirmed(..)) {
                return;
            }
        }
        panic!("dialog closed before ACK");
    })
    .await
    .unwrap();
    let request = dialog.prepare_reinvite(
        Some(vec![Header::ContentType("application/sdp".into())]),
        Some(OFFER.into()),
    )?;
    let sender = dialog.clone();
    let pending = tokio::spawn(async move {
        sender
            .send_reinvite(request, CancellationToken::new(), None)
            .await
    });
    let (request, address) = recv_request(&peer, Method::Invite).await;
    reply(&peer, address, &request, 488, "Not Acceptable Here").await;
    recv_request(&peer, Method::Ack).await;
    assert_eq!(pending.await.unwrap()?.unwrap().status_code.code(), 488);
    assert!(matches!(dialog.state(), DialogState::Confirmed(..)));
    Ok(())
}

#[tokio::test]
async fn incoming_reinvite_cancel_can_be_relayed_or_answered_by_default() -> crate::Result<()> {
    for observed in [false, true] {
        let token = CancellationToken::new();
        let _guard = token.clone().drop_guard();
        let (dialog, mut states, peer) = establish(&token).await?;
        let local = dialog
            .inner
            .endpoint_inner
            .transport_layer
            .get_listen_addrs()[0]
            .get_socketaddr()?;
        let remote = peer.local_addr()?;
        let id = dialog.id();
        let request = crate::sip::Request::try_from(format!(
            "INVITE sip:alice@{local} SIP/2.0\r\nVia: SIP/2.0/UDP {remote};branch=z9hG4bK-controlled-reinvite\r\nMax-Forwards: 70\r\nFrom: <sip:peer@{remote}>;tag={}\r\nTo: <sip:alice@{local}>;tag={}\r\nCall-ID: {}\r\nCSeq: 10 INVITE\r\nContact: <sip:peer@{remote}>\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\n\r\n{}",
            id.remote_tag, id.local_tag, id.call_id, OFFER.len(), std::str::from_utf8(OFFER).unwrap()
        ).as_str())?;
        peer.send_to(request.to_string().as_bytes(), local).await?;
        let handle = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = states.recv().await {
                if let DialogState::Updated(_, _, handle) = event {
                    return handle;
                }
            }
            panic!("missing re-INVITE event");
        })
        .await
        .unwrap();
        let cancellation = observed.then(|| handle.cancellation());
        let finished = handle.finished();
        handle.reply(180.into()).await.unwrap();
        receive_response(&peer, Method::Invite, 180).await?;
        let mut cancel = request.clone();
        cancel.method = Method::Cancel;
        cancel.headers.unique_push(Header::CSeq("10 CANCEL".into()));
        cancel.body.clear();
        cancel
            .headers
            .retain(|h| !matches!(h, Header::ContentType(_)));
        cancel.headers.unique_push(Header::ContentLength(0.into()));
        peer.send_to(cancel.to_string().as_bytes(), local).await?;
        receive_response(&peer, Method::Cancel, 200).await?;
        if let Some(cancellation) = cancellation {
            tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
                .await
                .unwrap();
            let mut bytes = [0; 8192];
            assert!(
                tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut bytes))
                    .await
                    .is_err(),
                "observed CANCEL waits for the application final response"
            );
            handle.reply(487.into()).await.unwrap();
        }
        let (final_response, _) = receive_response(&peer, Method::Invite, 487).await?;
        tokio::time::timeout(Duration::from_secs(1), finished.cancelled())
            .await
            .unwrap();
        let mut ack = request;
        ack.method = Method::Ack;
        ack.headers.unique_push(Header::CSeq("10 ACK".into()));
        ack.headers
            .unique_push(Header::To(final_response.to_header()?.clone()));
        ack.headers.retain(|h| !matches!(h, Header::ContentType(_)));
        ack.headers.unique_push(Header::ContentLength(0.into()));
        ack.body.clear();
        peer.send_to(ack.to_string().as_bytes(), local).await?;
        assert!(matches!(dialog.state(), DialogState::Confirmed(..)));
    }
    Ok(())
}

//! Initial INVITE, explicit cancellation and response receipts over real UDP.
#![cfg(feature = "platform-tokio")]

use rsipstack::{
    dialog::{
        authenticate::Credential,
        dialog::DialogStateSender,
        dialog_layer::DialogLayer,
        initial_operation::CancelCompletion,
        invitation::InviteOption,
        invite_dialog::InviteDialog,
        operation::OperationError,
        persistence::{
            DialogCheckpoint, DialogCheckpointReason, DialogPersistence, DialogPersistenceConfig,
        },
        reliable::{
            IncomingPrack, PrackBody, PrackHandler, ReliableProvisionalConfig, ReliableResponse,
        },
        response_operation::ResponseError,
    },
    sip::{Header, HeadersExt, Method, Request, Response, SipMessage, StatusCode},
    transaction::{
        endpoint::{EndpointInnerRef, EndpointOption},
        EndpointBuilder,
    },
    transport::{udp::UdpConnection, TransportLayer},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
use rsipstack::dialog::termination::TerminationOptions;
const DEADLINE: Duration = Duration::from_secs(3);

/// RustPBX currently handles this case in its initial transaction owner.
#[tokio::test]
async fn owned_early_bye_completes_the_original_invite() -> Result {
    let mut node = Node::with_owned(None, true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let mut initial = node.layer.make_invite_request(&node.options(node.local)?)?;
    initial.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-early-bye;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    initial.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    peer.send_to(initial.to_string().as_bytes(), node.local)
        .await?;
    let incoming = tokio::time::timeout(DEADLINE, node.owned.recv())
        .await?
        .ok_or("missing owned incoming INVITE")?;
    let dialog = incoming.dialog();
    dialog
        .start_response(StatusCode::Ringing, None, None)?
        .wait_sent()
        .await?;
    let ringing = response(&peer, StatusCode::Ringing).await?;
    let mut bye = initial.clone();
    bye.method = Method::Bye;
    bye.headers
        .unique_push(Header::To(ringing.to_header()?.clone()));
    bye.headers.unique_push(Header::CSeq(
        format!("{} BYE", initial.cseq_header()?.seq()? + 1).into(),
    ));
    bye.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-early-bye-end;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    peer.send_to(bye.to_string().as_bytes(), node.local).await?;
    let mut bye_ok = false;
    let mut invite_final = None;
    tokio::time::timeout(DEADLINE, async {
        while !bye_ok || invite_final.is_none() {
            let (message, _) = message(&peer).await?;
            if let SipMessage::Response(response) = message {
                match response.cseq_header()?.method()? {
                    Method::Bye => {
                        assert_eq!(response.status_code, StatusCode::OK);
                        bye_ok = true;
                    }
                    Method::Invite if response.status_code == StatusCode::RequestTerminated => {
                        invite_final = Some(response);
                    }
                    _ => {}
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    let invite_final = invite_final.unwrap();
    assert_eq!(invite_final.to_header()?, ringing.to_header()?);
    assert!(dialog.start_response(StatusCode::OK, None, None).is_err());
    // Lose the first 487: the owned driver must retain retransmission and ACK
    // processing even though the dialog has already ended.
    let repeated = response(&peer, StatusCode::RequestTerminated).await?;
    assert_eq!(repeated.to_string(), invite_final.to_string());
    let ack = node.endpoint.make_ack(&initial, &invite_final)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, incoming.wait()).await??;
    assert!(dialog.state().is_terminated());
    Ok(())
}

async fn owned_incoming_call(
    node: &mut Node,
    peer: &UdpSocket,
) -> Result<(Request, rsipstack::dialog::incoming::IncomingInvite)> {
    let mut request = node.layer.make_invite_request(&node.options(node.local)?)?;
    request.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-owned-bye;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    request.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    peer.send_to(request.to_string().as_bytes(), node.local)
        .await?;
    let incoming = tokio::time::timeout(DEADLINE, node.owned.recv())
        .await?
        .ok_or("missing owned INVITE")?;
    response(peer, StatusCode::Trying).await?;
    Ok((request, incoming))
}

fn early_bye(initial: &Request, early: &Response, peer: SocketAddr) -> Result<Request> {
    let mut bye = initial.clone();
    bye.method = Method::Bye;
    bye.headers
        .unique_push(Header::To(early.to_header()?.clone()));
    bye.headers.unique_push(Header::CSeq(
        format!("{} BYE", initial.cseq_header()?.seq()? + 1).into(),
    ));
    bye.headers.unique_push(Header::Via(
        format!("SIP/2.0/UDP {peer};branch=z9hG4bK-owned-bye-end;rport").into(),
    ));
    Ok(bye)
}

#[tokio::test]
async fn owned_early_bye_for_previous_branch_preserves_active_branch() -> Result {
    let mut node = Node::with_owned(None, true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (initial, incoming) = owned_incoming_call(&mut node, &peer).await?;
    let previous = incoming.branch()?;
    previous
        .start_response(StatusCode::Ringing, None, None)?
        .wait_sent()
        .await?;
    let old_early = response(&peer, StatusCode::Ringing).await?;
    let active = incoming.branch()?;
    active
        .start_response(StatusCode::SessionProgress, None, None)?
        .wait_sent()
        .await?;
    response(&peer, StatusCode::SessionProgress).await?;
    let bye = early_bye(&initial, &old_early, peer.local_addr()?)?;
    peer.send_to(bye.to_string().as_bytes(), node.local).await?;
    let bye_ok = response(&peer, StatusCode::OK).await?;
    assert_eq!(bye_ok.cseq_header()?.method()?, Method::Bye);
    assert!(previous.state().is_terminated());
    assert!(!active.state().is_terminated());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), message(&peer))
            .await
            .is_err()
    );
    let final_receipt = active.start_response(StatusCode::OK, None, None)?;
    let accepted = response(&peer, StatusCode::OK).await?;
    assert_eq!(
        accepted.to_header()?.tag()?.unwrap().value(),
        active.id().local_tag
    );
    let ack = node.endpoint.make_ack(&initial, &accepted)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, incoming.wait()).await??;
    final_receipt.wait_ack().await?;
    assert!(active.state().is_confirmed());
    Ok(())
}

#[tokio::test]
async fn owned_early_bye_for_selected_branch_rejects_with_its_identity() -> Result {
    let mut node = Node::with_owned(None, true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (initial, incoming) = owned_incoming_call(&mut node, &peer).await?;
    let selected = incoming.branch()?;
    selected
        .start_response(StatusCode::Ringing, None, None)?
        .wait_sent()
        .await?;
    let early = response(&peer, StatusCode::Ringing).await?;
    let bye = early_bye(&initial, &early, peer.local_addr()?)?;
    peer.send_to(bye.to_string().as_bytes(), node.local).await?;
    let rejected = response(&peer, StatusCode::RequestTerminated).await?;
    assert_eq!(rejected.to_header()?, early.to_header()?);
    let ack = node.endpoint.make_ack(&initial, &rejected)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, incoming.wait()).await??;
    assert!(selected.state().is_terminated());
    assert!(incoming.dialog().state().is_terminated());
    assert!(incoming.branch().is_err());
    Ok(())
}

struct SuccessGate {
    entered: CancellationToken,
    release: CancellationToken,
}
#[async_trait::async_trait]
impl DialogPersistence for SuccessGate {
    async fn save(&self, checkpoint: DialogCheckpoint) -> rsipstack::Result<()> {
        if checkpoint.reason == DialogCheckpointReason::BeforeResponse
            && matches!(&checkpoint.message, Some(SipMessage::Response(response))
                if response.status_code == StatusCode::OK
                    && response.cseq_header()?.method()? == Method::Invite)
        {
            self.entered.cancel();
            self.release.cancelled().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn owned_early_bye_does_not_replace_an_admitted_success_waiting_for_storage() -> Result {
    let gate = Arc::new(SuccessGate {
        entered: CancellationToken::new(),
        release: CancellationToken::new(),
    });
    let mut node = Node::with_owned(Some(gate.clone()), true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (initial, incoming) = owned_incoming_call(&mut node, &peer).await?;
    let dialog = incoming.dialog();
    dialog
        .start_response(StatusCode::Ringing, None, None)?
        .wait_sent()
        .await?;
    let early = response(&peer, StatusCode::Ringing).await?;
    let accepted = dialog.start_response(StatusCode::OK, None, None)?;
    tokio::time::timeout(DEADLINE, gate.entered.cancelled()).await?;
    let bye = early_bye(&initial, &early, peer.local_addr()?)?;
    peer.send_to(bye.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, async {
        while !dialog.state().is_terminated() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    gate.release.cancel();
    let mut got_bye = false;
    let mut got_invite = false;
    tokio::time::timeout(DEADLINE, async {
        while !got_bye || !got_invite {
            let (packet, _) = message(&peer).await?;
            if let SipMessage::Response(response) = packet {
                assert_ne!(response.status_code, StatusCode::RequestTerminated);
                match response.cseq_header()?.method()? {
                    Method::Bye => {
                        assert_eq!(response.status_code, StatusCode::OK);
                        got_bye = true;
                    }
                    Method::Invite if response.status_code == StatusCode::OK => {
                        got_invite = true;
                        let ack = node.endpoint.make_ack(&initial, &response)?;
                        peer.send_to(ack.to_string().as_bytes(), node.local).await?;
                    }
                    _ => {}
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    accepted.wait_sent().await?;
    tokio::time::timeout(DEADLINE, incoming.wait()).await??;
    assert!(dialog.state().is_terminated());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), message(&peer))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn termination_waits_for_matching_uas_ack_and_coalesces_observers() -> Result {
    let mut node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (invite, dialog, task) = incoming_call(&mut node, &peer).await?;
    // Terminate while the success is only admitted, before WaitAck is visible.
    let response_receipt = dialog.start_response(StatusCode::OK, None, None)?;
    let termination = dialog.start_termination(
        TerminationOptions::default().with_reason("SIP;cause=200;text=\"done\""),
    )?;
    let second = dialog.start_termination(Default::default())?;
    assert_eq!(termination.id(), second.id());
    let observer = termination.clone();
    let waiter = tokio::spawn(async move { observer.wait().await });
    waiter.abort();
    let success = response(&peer, StatusCode::OK).await?;
    response_receipt.wait_sent().await?;
    let ack = node.endpoint.make_ack(&invite, &success)?;
    let mut wrong = ack.clone();
    wrong
        .cseq_header_mut()?
        .mut_seq(invite.cseq_header()?.seq()? + 1)?;
    peer.send_to(wrong.to_string().as_bytes(), node.local)
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request(&peer, Method::Bye))
            .await
            .is_err()
    );
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    let (bye, source) = request(&peer, Method::Bye).await?;
    tokio::time::timeout(DEADLINE, async {
        while !dialog.state().is_terminated() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // Session termination must not wait for the BYE response, while the
    // operation still owns the exchange and cannot complete prematurely.
    assert!(
        tokio::time::timeout(Duration::from_millis(25), termination.wait())
            .await
            .is_err()
    );
    assert!(bye
        .headers
        .iter()
        .any(|h| matches!(h, Header::Reason(v) if v.value().contains("done"))));
    answer(&node, &peer, &bye, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(DEADLINE, termination.wait()).await?;
    result.signaling_result().map_err(Clone::clone)?;
    assert!(result.dialog_state().is_terminated());
    assert!(result.progress().bye.as_ref().unwrap().response().is_some());
    assert_eq!(
        dialog.start_termination(Default::default())?.id(),
        second.id()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request(&peer, Method::Bye))
            .await
            .is_err()
    );
    tokio::time::timeout(DEADLINE, task).await???;
    Ok(())
}

#[tokio::test]
async fn termination_missing_uas_ack_shares_one_bye_with_timeout_cleanup() -> Result {
    let mut node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (_, dialog, task) = incoming_call(&mut node, &peer).await?;
    dialog
        .start_response(StatusCode::OK, None, None)?
        .wait_sent()
        .await?;
    response(&peer, StatusCode::OK).await?;
    let started = tokio::time::Instant::now();
    let termination = dialog.start_termination(Default::default())?;
    let (bye, source) = request(&peer, Method::Bye).await?;
    assert!(started.elapsed() >= Duration::from_millis(500));
    answer(&node, &peer, &bye, source, StatusCode::OK).await?;
    let completed = tokio::time::timeout(DEADLINE, termination.wait()).await?;
    completed.signaling_result().map_err(Clone::clone)?;
    assert!(matches!(
        completed.dialog_state(),
        rsipstack::dialog::dialog::DialogState::Terminated(
            _,
            rsipstack::dialog::dialog::TerminatedReason::Timeout
        )
    ));
    tokio::time::timeout(DEADLINE, task).await???;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request(&peer, Method::Bye))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn termination_rejects_early_uas_and_reports_lost_ack_owner() -> Result {
    let mut node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (invite, dialog, task) = incoming_call(&mut node, &peer).await?;
    let operation = dialog
        .start_termination(TerminationOptions::default().with_rejection(StatusCode::BusyHere))?;
    let rejected = response(&peer, StatusCode::BusyHere).await?;
    let completed = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    completed.signaling_result().map_err(Clone::clone)?;
    assert!(
        completed
            .progress()
            .rejection
            .as_ref()
            .unwrap()
            .transport_accepted
    );
    let ack = node.endpoint.make_ack(&invite, &rejected)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, task).await???;

    let mut node = Node::new(None).await?;
    let (_, dialog, task) = incoming_call(&mut node, &peer).await?;
    dialog
        .start_response(StatusCode::OK, None, None)?
        .wait_sent()
        .await?;
    let operation = dialog.start_termination(Default::default())?;
    task.abort();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, operation.wait())
            .await?
            .signaling_result(),
        Err(OperationError::DriverStopped)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request(&peer, Method::Bye))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn termination_cancels_early_uac_and_retains_late_success_and_cleanup() -> Result {
    for success in [false, true] {
        let node = Node::new(None).await?;
        let peer = UdpSocket::bind("127.0.0.1:0").await?;
        let (dialog, _initial) = node
            .layer
            .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
        let (invite, source) = request(&peer, Method::Invite).await?;
        let termination = dialog.start_termination(
            TerminationOptions::default().with_reason("SIP;cause=200;text=\"stop\""),
        )?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), request(&peer, Method::Cancel))
                .await
                .is_err()
        );
        answer(&node, &peer, &invite, source, StatusCode::Trying).await?;
        let (cancel, _) = request(&peer, Method::Cancel).await?;
        assert!(cancel
            .headers
            .iter()
            .any(|h| matches!(h, Header::Reason(_))));
        answer(&node, &peer, &cancel, source, StatusCode::OK).await?;
        let final_code = if success {
            StatusCode::OK
        } else {
            StatusCode::RequestTerminated
        };
        answer(&node, &peer, &invite, source, final_code.clone()).await?;
        request(&peer, Method::Ack).await?;
        if success {
            let (bye, source) = request(&peer, Method::Bye).await?;
            assert!(bye.headers.iter().any(|h| matches!(h, Header::Reason(_))));
            answer(&node, &peer, &bye, source, StatusCode::OK).await?;
        }
        let completed = tokio::time::timeout(DEADLINE, termination.wait()).await?;
        completed.signaling_result().map_err(Clone::clone)?;
        assert_eq!(
            completed
                .progress()
                .initial_invite
                .as_ref()
                .unwrap()
                .response()
                .unwrap()
                .status_code,
            final_code
        );
        assert_eq!(completed.progress().bye.is_some(), success);
        assert!(completed.dialog_state().is_terminated());
    }
    Ok(())
}

#[tokio::test]
async fn termination_before_initial_poll_sends_nothing() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (dialog, _initial) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let operation = dialog.start_termination(Default::default())?;
    let completed = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    completed.signaling_result().map_err(Clone::clone)?;
    assert!(completed.dialog_state().is_terminated());
    assert!(matches!(
        completed.progress().cancellation,
        Some(CancelCompletion::SuppressedBeforeSend)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), message(&peer))
            .await
            .is_err()
    );
    Ok(())
}

struct Node {
    stop: CancellationToken,
    endpoint: EndpointInnerRef,
    layer: Arc<DialogLayer>,
    local: SocketAddr,
    states: DialogStateSender,
    incoming: mpsc::UnboundedReceiver<(InviteDialog, JoinHandle<rsipstack::Result<()>>)>,
    admitted: mpsc::UnboundedReceiver<(Request, rsipstack::dialog::dialog::TransactionHandle)>,
    owned: mpsc::UnboundedReceiver<rsipstack::dialog::incoming::IncomingInvite>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Node {
    async fn new(storage: Option<Arc<dyn DialogPersistence>>) -> Result<Self> {
        Self::with_owned(storage, false).await
    }
    async fn with_owned(
        storage: Option<Arc<dyn DialogPersistence>>,
        owned_driver: bool,
    ) -> Result<Self> {
        let stop = CancellationToken::new();
        let transport = TransportLayer::new(stop.clone());
        let udp =
            UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone()))
                .await?;
        let local = udp.get_addr().get_socketaddr()?;
        transport.add_transport(udp.into());
        let option = EndpointOption {
            t1: Duration::from_millis(80),
            t1x64: Duration::from_millis(600),
            // Managed INVITEs must own ACK even when the raw endpoint is also
            // configured for proxy transactions.
            auto_ack_2xx: false,
            ..Default::default()
        };
        let endpoint = EndpointBuilder::new()
            .with_transport_layer(transport)
            .with_option(option)
            .with_cancel_token(stop.clone())
            .build();
        let layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
        if let Some(storage) = storage {
            layer.set_persistence(Some(DialogPersistenceConfig::new(storage)))?;
        }
        let (states, mut state_rx) = layer.new_dialog_state_channel();
        let (requests, admitted) = mpsc::unbounded_channel();
        let state_stop = stop.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = state_stop.cancelled() => break,
                    state = state_rx.recv() => match state {
                        Some(rsipstack::dialog::dialog::DialogState::Info(_, req, handle)
                            | rsipstack::dialog::dialog::DialogState::Updated(_, req, handle)) => { requests.send((req,handle)).ok(); }
                        None => break,
                        _ => {}
                    },
                }
            }
        });
        let mut transactions = endpoint.incoming_transactions()?;
        let (incoming_tx, incoming) = mpsc::unbounded_channel();
        let (owned_tx, owned) = mpsc::unbounded_channel();
        let router = layer.clone();
        let router_states = states.clone();
        let router_stop = stop.clone();
        tokio::spawn(async move {
            loop {
                let mut tx = tokio::select! {
                    _ = router_stop.cancelled() => break,
                    tx = transactions.recv() => match tx { Some(tx) => tx, None => break },
                };
                if let Some(dialog) = router.match_dialog(&tx) {
                    let operation = dialog.start_incoming_request(tx).unwrap();
                    tokio::spawn(async move { operation.wait().await });
                } else if tx.original.method == Method::Invite {
                    if owned_driver {
                        let call = router
                            .start_incoming_invite(
                                tx,
                                router_states.clone(),
                                Some(format!("sip:server@{local}").parse().unwrap()),
                            )
                            .unwrap();
                        owned_tx.send(call).unwrap();
                        continue;
                    }
                    let dialog = router
                        .get_or_create_server_invite(
                            &tx,
                            router_states.clone(),
                            None,
                            Some(format!("sip:server@{local}").parse().unwrap()),
                        )
                        .unwrap();
                    let observed = dialog.clone();
                    // Explicit borrowed-driver coverage for legacy receipts and
                    // owner-loss behavior; the managed public API owns its task.
                    #[allow(deprecated)]
                    let task = tokio::spawn(async move {
                        let mut legacy =
                            rsipstack::dialog::server_dialog::ServerInviteDialog::try_from(dialog)?;
                        legacy.handle(&mut tx).await
                    });
                    incoming_tx.send((observed, task)).unwrap();
                } else {
                    tx.reply(StatusCode::CallTransactionDoesNotExist).await.ok();
                }
            }
        });
        let inner = endpoint.inner.clone();
        tokio::spawn(async move { endpoint.serve().await });
        Ok(Self {
            stop,
            endpoint: inner,
            layer,
            local,
            states,
            incoming,
            owned,
            admitted,
        })
    }
    fn options(&self, target: SocketAddr) -> Result<InviteOption> {
        Ok(InviteOption {
            caller: format!("sip:caller@{}", self.local).parse()?,
            callee: format!("sip:callee@{target}").parse()?,
            contact: format!("sip:caller@{}", self.local).parse()?,
            ..Default::default()
        })
    }
    async fn incoming(&mut self) -> Result<(InviteDialog, JoinHandle<rsipstack::Result<()>>)> {
        tokio::time::timeout(DEADLINE, self.incoming.recv())
            .await?
            .ok_or_else(|| "incoming closed".into())
    }
}
async fn message(peer: &UdpSocket) -> Result<(SipMessage, SocketAddr)> {
    let mut bytes = [0; 8192];
    let (size, source) = tokio::time::timeout(DEADLINE, peer.recv_from(&mut bytes)).await??;
    Ok((std::str::from_utf8(&bytes[..size])?.try_into()?, source))
}
async fn request(peer: &UdpSocket, method: Method) -> Result<(Request, SocketAddr)> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let (SipMessage::Request(request), source) = message(peer).await? {
                if request.method == method {
                    return Ok((request, source));
                }
            }
        }
    })
    .await?
}
async fn response(peer: &UdpSocket, status: StatusCode) -> Result<Response> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let (SipMessage::Response(response), _) = message(peer).await? {
                if response.status_code == status {
                    return Ok(response);
                }
            }
        }
    })
    .await?
}
async fn answer(
    node: &Node,
    peer: &UdpSocket,
    req: &Request,
    source: SocketAddr,
    status: StatusCode,
) -> Result<Response> {
    let mut response = node.endpoint.make_response(req, status, None);
    if response.status_code != StatusCode::Trying && response.to_header()?.tag()?.is_none() {
        response.to_header_mut()?.mut_tag("peer".into())?;
    }
    response.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    peer.send_to(response.to_string().as_bytes(), source)
        .await?;
    Ok(response)
}
async fn incoming_call(
    node: &mut Node,
    peer: &UdpSocket,
) -> Result<(Request, InviteDialog, JoinHandle<rsipstack::Result<()>>)> {
    let request = node.layer.make_invite_request(&node.options(node.local)?)?;
    let mut request = request;
    request.headers.unique_push(Header::Via(
        format!("SIP/2.0/UDP {};branch=z9hG4bK-receipt", peer.local_addr()?).into(),
    ));
    request.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    request
        .headers
        .unique_push(Header::Supported("100rel".into()));
    peer.send_to(request.to_string().as_bytes(), node.local)
        .await?;
    let (dialog, task) = node.incoming().await?;
    response(peer, StatusCode::Trying).await?;
    Ok((request, dialog, task))
}

#[tokio::test]
async fn initial_invite_survives_dropped_waiter_and_owns_ack() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (dialog, operation) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let observer = operation.clone();
    let waiter = tokio::spawn(async move { observer.wait().await });
    let (invite, source) = request(&peer, Method::Invite).await?;
    assert!(operation
        .operation()
        .wait_first_send()
        .await?
        .transport_accepted());
    waiter.abort();
    answer(&node, &peer, &invite, source, StatusCode::OK).await?;
    let (ack, _) = request(&peer, Method::Ack).await?;
    assert_eq!(ack.cseq_header()?.seq()?, invite.cseq_header()?.seq()?);
    let result = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    assert!(
        result.signaling_result().is_ok(),
        "{:?}",
        result.signaling_result()
    );
    assert_eq!(
        result.response().map(|r| r.status_code.clone()),
        Some(StatusCode::OK)
    );
    assert!(result.attempts()[0].ack_sent());
    assert!(dialog.state().is_confirmed());
    assert!(dialog.snapshot().is_quiescent());
    Ok(())
}

#[tokio::test]
async fn initial_cancel_is_admitted_before_any_waiter_is_polled() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (dialog, operation) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let cancellation = operation.cancel();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, cancellation.wait()).await?,
        CancelCompletion::SuppressedBeforeSend
    ));
    let result = operation.wait().await;
    assert!(matches!(
        result.signaling_result(),
        Err(OperationError::CancelledBeforeSend)
    ));
    assert!(dialog.state().is_terminated());
    assert!(dialog.snapshot().runtime.pending.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(40), message(&peer))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cancel_waits_for_provisional_and_retains_separate_invite_result() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (_, operation) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let (invite, source) = request(&peer, Method::Invite).await?;
    let cancel = operation.cancel();
    let same_cancel = operation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), message(&peer))
            .await
            .is_err()
    );
    answer(&node, &peer, &invite, source, StatusCode::Trying).await?;
    let (cancel_request, source) = request(&peer, Method::Cancel).await?;
    assert_eq!(
        cancel_request.cseq_header()?.seq()?,
        invite.cseq_header()?.seq()?
    );
    assert_eq!(cancel_request.via_header()?, invite.via_header()?);
    answer(&node, &peer, &cancel_request, source, StatusCode::OK).await?;
    answer(&node, &peer, &invite, source, StatusCode::RequestTerminated).await?;
    request(&peer, Method::Ack).await?;
    let cancel_result = tokio::time::timeout(DEADLINE, cancel.wait()).await?;
    assert_eq!(
        cancel_result.response().map(|r| r.status_code.clone()),
        Some(StatusCode::OK)
    );
    assert_eq!(
        same_cancel.wait().await.response(),
        cancel_result.response()
    );
    let result = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    assert_eq!(
        result.response().map(|r| r.status_code.clone()),
        Some(StatusCode::RequestTerminated)
    );
    assert!(result.signaling_result().is_ok());
    Ok(())
}

#[tokio::test]
async fn cancellation_after_authentication_waits_for_the_new_attempt_provisional() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let mut options = node.options(peer.local_addr()?)?;
    options.credential = Some(Credential {
        username: "user".into(),
        password: "secret".into(),
        ..Default::default()
    });
    let (dialog, operation) = node.layer.start_invite(options, node.states.clone())?;
    let (first, source) = request(&peer, Method::Invite).await?;
    answer(&node, &peer, &first, source, StatusCode::Trying).await?;
    let mut challenge = node
        .endpoint
        .make_response(&first, StatusCode::Unauthorized, None);
    challenge.to_header_mut()?.mut_tag("challenge".into())?;
    challenge.headers.unique_push(Header::WwwAuthenticate(
        "Digest realm=\"test\", nonce=\"one\", algorithm=MD5, qop=\"auth\"".into(),
    ));
    peer.send_to(challenge.to_string().as_bytes(), source)
        .await?;
    request(&peer, Method::Ack).await?;
    let (second, source) = request(&peer, Method::Invite).await?;
    assert!(second.cseq_header()?.seq()? > first.cseq_header()?.seq()?);
    let cancellation = dialog.start_cancel()?;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), message(&peer))
            .await
            .is_err()
    );
    answer(&node, &peer, &second, source, StatusCode::Trying).await?;
    let (cancel, source) = request(&peer, Method::Cancel).await?;
    assert_eq!(cancel.cseq_header()?.seq()?, second.cseq_header()?.seq()?);
    assert_eq!(cancel.via_header()?, second.via_header()?);
    answer(&node, &peer, &cancel, source, StatusCode::OK).await?;
    answer(&node, &peer, &second, source, StatusCode::RequestTerminated).await?;
    request(&peer, Method::Ack).await?;
    let completed = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    assert_eq!(completed.attempts().len(), 2);
    assert_eq!(
        completed.attempts()[0].response().unwrap().status_code,
        StatusCode::Unauthorized
    );
    assert_eq!(
        completed.response().unwrap().status_code,
        StatusCode::RequestTerminated
    );
    assert!(completed
        .attempts()
        .iter()
        .all(|attempt| attempt.ack_sent()));
    assert!(cancellation.wait().await.signaling_result().is_ok());
    Ok(())
}

#[tokio::test]
async fn late_success_after_cancel_is_acked_then_terminated_before_completion() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (_, operation) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let (invite, source) = request(&peer, Method::Invite).await?;
    let cancel = operation.cancel();
    answer(&node, &peer, &invite, source, StatusCode::Trying).await?;
    let (cancel_request, _) = request(&peer, Method::Cancel).await?;
    answer(&node, &peer, &cancel_request, source, StatusCode::OK).await?;
    let success = answer(&node, &peer, &invite, source, StatusCode::OK).await?;
    let (first, _) = message(&peer).await?;
    assert!(matches!(first, SipMessage::Request(r) if r.method == Method::Ack));
    let (bye, _) = request(&peer, Method::Bye).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), operation.wait())
            .await
            .is_err()
    );
    peer.send_to(success.to_string().as_bytes(), source).await?;
    request(&peer, Method::Ack).await?;
    answer(&node, &peer, &bye, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(DEADLINE, operation.wait()).await?;
    assert_eq!(
        result.response().map(|r| r.status_code.clone()),
        Some(StatusCode::OK)
    );
    assert!(result.dialog_state().is_terminated());
    assert!(
        result.signaling_result().is_ok(),
        "{:?}",
        result.signaling_result()
    );
    assert!(cancel.wait().await.signaling_result().is_ok());
    Ok(())
}

#[tokio::test]
async fn uas_receipt_separates_send_from_matching_ack_and_survives_dropped_waiter() -> Result {
    let mut node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (invite, dialog, task) = incoming_call(&mut node, &peer).await?;
    let ringing = dialog.start_response(StatusCode::Ringing, None, None)?;
    ringing.wait_sent().await?;
    response(&peer, StatusCode::Ringing).await?;
    assert!(matches!(
        ringing.wait_ack().await,
        Err(ResponseError::AckNotApplicable)
    ));
    let receipt = dialog.start_response(StatusCode::OK, None, None)?;
    assert!(matches!(
        dialog.start_response(StatusCode::BusyHere, None, None),
        Err(ResponseError::FinalAlreadyAdmitted)
    ));
    let observer = receipt.clone();
    let waiter = tokio::spawn(async move { observer.wait_ack().await });
    let success = response(&peer, StatusCode::OK).await?;
    waiter.abort();
    assert!(receipt.wait_sent().await?.transport_accepted);
    assert!(dialog.state().waiting_ack());
    let ack = node.endpoint.make_ack(&invite, &success)?;
    let mut wrong = ack.clone();
    wrong
        .cseq_header_mut()?
        .mut_seq(invite.cseq_header()?.seq()? + 1)?;
    peer.send_to(wrong.to_string().as_bytes(), node.local)
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), receipt.wait_ack())
            .await
            .is_err()
    );
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    let received = tokio::time::timeout(DEADLINE, receipt.wait_ack()).await??;
    assert_eq!(received.cseq_header()?.seq()?, invite.cseq_header()?.seq()?);
    tokio::time::timeout(DEADLINE, task).await???;
    assert!(dialog.state().is_confirmed());
    Ok(())
}

struct ReliableMedia;
#[async_trait::async_trait]
impl PrackHandler for ReliableMedia {
    async fn on_reliable_response(
        &self,
        _: ReliableResponse,
    ) -> rsipstack::Result<Option<PrackBody>> {
        Ok(None)
    }
    async fn on_prack(&self, _: IncomingPrack) -> rsipstack::Result<Option<PrackBody>> {
        Ok(None)
    }
}

#[tokio::test]
async fn final_response_receipt_remains_pending_while_waiting_for_prack() -> Result {
    let mut node = Node::new(None).await?;
    node.layer
        .set_reliable_provisional(Some(ReliableProvisionalConfig::new(Arc::new(
            ReliableMedia,
        ))));
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (invite, dialog, task) = incoming_call(&mut node, &peer).await?;
    let sdp = b"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=test\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 9000 RTP/AVP 0\r\na=inactive\r\n".to_vec();
    let headers = Some(vec![Header::ContentType("application/sdp".into())]);
    let provisional = dialog.start_response(
        StatusCode::SessionProgress,
        headers.clone(),
        Some(sdp.clone()),
    )?;
    provisional.wait_sent().await?;
    let early = response(&peer, StatusCode::SessionProgress).await?;
    let accepted = dialog.start_response(StatusCode::OK, headers, Some(sdp.clone()))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), accepted.wait_sent())
            .await
            .is_err()
    );
    assert!(!accepted.progress().transport_accepted);
    let mut prack = invite.clone();
    prack.method = Method::PRack;
    prack
        .cseq_header_mut()?
        .mut_seq(invite.cseq_header()?.seq()? + 1)?
        .mut_method(Method::PRack)?;
    prack
        .headers
        .unique_push(Header::To(early.to_header()?.clone()));
    prack.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-receipt-prack",
            peer.local_addr()?
        )
        .into(),
    ));
    prack.headers.unique_push(Header::RAck(
        format!(
            "{} {} INVITE",
            early.rseq_value().ok_or("missing RSeq")?,
            invite.cseq_header()?.seq()?
        )
        .into(),
    ));
    prack.body = sdp;
    prack
        .headers
        .unique_push(Header::ContentType("application/sdp".into()));
    prack
        .headers
        .unique_push(Header::ContentLength(prack.body.len().to_string().into()));
    peer.send_to(prack.to_string().as_bytes(), node.local)
        .await?;
    let sent = tokio::time::timeout(DEADLINE, accepted.wait_sent()).await??;
    assert!(sent.transport_accepted);
    let final_response = loop {
        let received = response(&peer, StatusCode::OK).await?;
        if received.cseq_header()?.method()? == Method::Invite {
            break received;
        }
    };
    let ack = node.endpoint.make_ack(&invite, &final_response)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, accepted.wait_ack()).await??;
    tokio::time::timeout(DEADLINE, task).await???;
    Ok(())
}

#[tokio::test]
async fn uas_receipt_reports_missing_ack_and_lost_owner() -> Result {
    let mut node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (_, dialog, task) = incoming_call(&mut node, &peer).await?;
    let receipt = dialog.start_response(StatusCode::OK, None, None)?;
    receipt.wait_sent().await?;
    assert!(matches!(
        tokio::time::timeout(DEADLINE, receipt.wait_ack()).await?,
        Err(ResponseError::AckTimeout)
    ));
    task.abort();
    let mut node = Node::new(None).await?;
    let (_, dialog, task) = incoming_call(&mut node, &peer).await?;
    let receipt = dialog.start_response(StatusCode::OK, None, None)?;
    receipt.wait_sent().await?;
    task.abort();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, receipt.wait_ack()).await?,
        Err(ResponseError::DriverStopped)
    ));
    Ok(())
}

struct Gate {
    entered: CancellationToken,
    release: CancellationToken,
    fail: bool,
}

#[tokio::test]
async fn termination_observes_storage_failure_or_waits_for_persisted_success_and_ack() -> Result {
    for fail in [false, true] {
        let storage = Arc::new(Gate {
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            fail,
        });
        let mut node = Node::new(Some(storage.clone())).await?;
        let peer = UdpSocket::bind("127.0.0.1:0").await?;
        let (invite, dialog, task) = incoming_call(&mut node, &peer).await?;
        let receipt = dialog.start_response(StatusCode::OK, None, None)?;
        tokio::time::timeout(DEADLINE, storage.entered.cancelled()).await?;
        let operation = dialog.start_termination(Default::default())?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), operation.wait())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), message(&peer))
                .await
                .is_err()
        );
        storage.release.cancel();
        if fail {
            assert!(matches!(
                receipt.wait_sent().await,
                Err(ResponseError::Persistence(_))
            ));
            let completed = tokio::time::timeout(DEADLINE, operation.wait()).await?;
            assert!(matches!(
                completed.signaling_result(),
                Err(OperationError::Persistence(_))
            ));
            assert!(completed.progress().bye.is_none());
            assert!(
                tokio::time::timeout(Duration::from_millis(25), message(&peer))
                    .await
                    .is_err()
            );
        } else {
            receipt.wait_sent().await?;
            let success = response(&peer, StatusCode::OK).await?;
            let ack = node.endpoint.make_ack(&invite, &success)?;
            peer.send_to(ack.to_string().as_bytes(), node.local).await?;
            let (bye, source) = request(&peer, Method::Bye).await?;
            answer(&node, &peer, &bye, source, StatusCode::OK).await?;
            let completed = tokio::time::timeout(DEADLINE, operation.wait()).await?;
            completed.signaling_result().map_err(Clone::clone)?;
            assert!(dialog.persistence_status().unwrap().failure_kind.is_none());
        }
        task.abort();
    }
    Ok(())
}
#[async_trait::async_trait]
impl DialogPersistence for Gate {
    async fn save(&self, checkpoint: DialogCheckpoint) -> rsipstack::Result<()> {
        if checkpoint.reason == DialogCheckpointReason::BeforeResponse {
            self.entered.cancel();
            self.release.cancelled().await;
            if self.fail {
                return Err(rsipstack::Error::Error(
                    "test storage rejected response".into(),
                ));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn uas_receipt_waits_for_storage_and_retains_failure_without_claiming_send() -> Result {
    for fail in [false, true] {
        let storage = Arc::new(Gate {
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            fail,
        });
        let mut node = Node::new(Some(storage.clone())).await?;
        let peer = UdpSocket::bind("127.0.0.1:0").await?;
        let (_, dialog, task) = incoming_call(&mut node, &peer).await?;
        let receipt = dialog.start_response(StatusCode::OK, None, None)?;
        tokio::time::timeout(DEADLINE, storage.entered.cancelled()).await?;
        assert!(!receipt.progress().transport_accepted);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), message(&peer))
                .await
                .is_err()
        );
        storage.release.cancel();
        let sent = tokio::time::timeout(DEADLINE, receipt.wait_sent()).await?;
        if fail {
            assert!(matches!(sent, Err(ResponseError::Persistence(_))));
            assert!(!receipt.progress().transport_accepted);
            assert!(matches!(
                receipt.wait_ack().await,
                Err(ResponseError::Persistence(_))
            ));
        } else {
            let sent = sent?;
            assert!(
                sent.transport_accepted && sent.checkpoint_committed && sent.completion_committed
            );
            response(&peer, StatusCode::OK).await?;
        }
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn owned_incoming_branches_keep_identity_and_one_final_response() -> Result {
    let mut node = Node::with_owned(None, true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let mut invite = node.layer.make_invite_request(&node.options(node.local)?)?;
    invite.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-owned-branches;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    invite.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    peer.send_to(invite.to_string().as_bytes(), node.local)
        .await?;
    let incoming = tokio::time::timeout(DEADLINE, node.owned.recv())
        .await?
        .ok_or("no owned INVITE")?;
    response(&peer, StatusCode::Trying).await?;
    let first = incoming.branch()?;
    first
        .start_response(
            StatusCode::SessionProgress,
            None,
            Some(b"first offer".to_vec()),
        )?
        .wait_sent()
        .await?;
    let early = response(&peer, StatusCode::SessionProgress).await?;
    assert_eq!(
        early.to_header()?.tag()?.unwrap().value(),
        first.id().local_tag
    );
    let second = incoming.branch()?;
    assert_ne!(first.id(), second.id());
    second
        .start_response(
            StatusCode::SessionProgress,
            None,
            Some(b"second offer".to_vec()),
        )?
        .wait_sent()
        .await?;
    let early = response(&peer, StatusCode::SessionProgress).await?;
    assert_eq!(
        early.to_header()?.tag()?.unwrap().value(),
        second.id().local_tag
    );
    let receipt = second.start_response(StatusCode::OK, None, Some(b"second offer".to_vec()))?;
    assert!(matches!(
        first.start_response(StatusCode::BusyHere, None, None),
        Err(ResponseError::FinalAlreadyAdmitted)
    ));
    assert!(matches!(
        incoming.branch(),
        Err(ResponseError::FinalAlreadyAdmitted)
    ));
    drop(incoming);
    let success = response(&peer, StatusCode::OK).await?;
    receipt.wait_sent().await?;
    assert!(first.state().is_terminated());
    assert!(node.layer.get_dialog(&first.id()).is_none());
    assert!(node.layer.get_dialog(&second.id()).is_some());
    let ack = node.endpoint.make_ack(&invite, &success)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    tokio::time::timeout(DEADLINE, receipt.wait_ack()).await??;
    tokio::time::timeout(DEADLINE, async {
        while !second.state().is_confirmed() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let terminate = second.start_termination(Default::default())?;
    let (bye, source) = request(&peer, Method::Bye).await?;
    assert_eq!(
        bye.from_header()?.tag()?.unwrap().value(),
        second.id().local_tag
    );
    answer(&node, &peer, &bye, source, StatusCode::OK).await?;
    tokio::time::timeout(DEADLINE, terminate.wait())
        .await?
        .signaling_result()
        .map_err(Clone::clone)?;
    Ok(())
}

#[tokio::test]
async fn late_success_after_failed_initial_invite_is_acked_and_released() -> Result {
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (_, operation) = node
        .layer
        .start_invite(node.options(peer.local_addr()?)?, node.states.clone())?;
    let (invite, source) = request(&peer, Method::Invite).await?;
    answer(&node, &peer, &invite, source, StatusCode::BusyHere).await?;
    request(&peer, Method::Ack).await?;
    assert_eq!(
        operation.wait().await.response().unwrap().status_code,
        StatusCode::BusyHere
    );
    let success = answer(&node, &peer, &invite, source, StatusCode::OK).await?;
    let (ack, _) = request(&peer, Method::Ack).await?;
    assert_eq!(ack.to_header()?, success.to_header()?);
    let (bye, _) = request(&peer, Method::Bye).await?;
    answer(&node, &peer, &bye, source, StatusCode::OK).await?;
    peer.send_to(success.to_string().as_bytes(), source).await?;
    assert_eq!(
        request(&peer, Method::Ack).await?.0.to_string(),
        ack.to_string()
    );
    assert_eq!(
        operation.wait().await.response().unwrap().status_code,
        StatusCode::BusyHere
    );
    Ok(())
}

#[tokio::test]
async fn initial_request_uri_and_raw_provisional_observer_are_independent_of_negotiation() -> Result
{
    let node = Node::new(None).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (send, mut progress) = mpsc::unbounded_channel();
    let mut options = node.options(peer.local_addr()?)?;
    options.request_uri = Some(format!("sip:prefix-callee@{}", peer.local_addr()?).parse()?);
    options.provisional_responses = Some(send);
    let (_, operation) = node.layer.start_invite(options, node.states.clone())?;
    let (invite, source) = request(&peer, Method::Invite).await?;
    assert!(invite.uri.to_string().contains("prefix-callee@"));
    assert!(!invite.to_header()?.value().contains("prefix-callee@"));
    let mut provisional = node
        .endpoint
        .make_response(&invite, StatusCode::SessionProgress, None);
    provisional
        .headers
        .unique_push(Header::Require("100rel".into()));
    peer.send_to(provisional.to_string().as_bytes(), source)
        .await?;
    let observed = tokio::time::timeout(DEADLINE, progress.recv())
        .await?
        .ok_or("no provisional")?;
    assert_eq!(observed.status_code, StatusCode::SessionProgress);
    let cancel = operation.cancel();
    let (cancel_request, _) = request(&peer, Method::Cancel).await?;
    answer(&node, &peer, &cancel_request, source, StatusCode::OK).await?;
    answer(&node, &peer, &invite, source, StatusCode::RequestTerminated).await?;
    cancel
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_incoming_admits_sequences_in_order_and_delivers_ack_before_next_invite() -> Result {
    let mut node = Node::with_owned(None, true).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let mut initial = node.layer.make_invite_request(&node.options(node.local)?)?;
    initial.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-owned-burst;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    initial.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", peer.local_addr()?).into(),
    ));
    initial
        .headers
        .unique_push(Header::CSeq("41 INVITE".into()));
    peer.send_to(initial.to_string().as_bytes(), node.local)
        .await?;
    let incoming = tokio::time::timeout(DEADLINE, node.owned.recv())
        .await?
        .ok_or("no initial exchange")?;
    let dialog = incoming.dialog();
    assert_eq!(dialog.snapshot().remote_cseq, 41);
    dialog
        .start_response(StatusCode::OK, None, None)?
        .wait_sent()
        .await?;
    let success = response(&peer, StatusCode::OK).await?;
    let ack = node.endpoint.make_ack(&initial, &success)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    incoming.wait().await?;
    let make = |method: Method, seq: u32| -> Result<Request> {
        let mut req = ack.clone();
        req.method = method.clone();
        req.headers
            .unique_push(Header::CSeq(format!("{seq} {method}").into()));
        req.headers.unique_push(Header::Via(
            format!(
                "SIP/2.0/UDP {};branch=z9hG4bK-{method}-{seq};rport",
                peer.local_addr()?
            )
            .into(),
        ));
        req.headers.unique_push(Header::Contact(
            format!("<sip:peer@{}>", peer.local_addr()?).into(),
        ));
        Ok(req)
    };
    for seq in 42..=44 {
        peer.send_to(make(Method::Info, seq)?.to_string().as_bytes(), node.local)
            .await?;
    }
    let mut handles = Vec::new();
    let mut accepted_sequences = Vec::new();
    for _ in 42..=44 {
        let (request, handle) = tokio::time::timeout(DEADLINE, node.admitted.recv())
            .await?
            .ok_or("no INFO")?;
        accepted_sequences.push(request.cseq_header()?.seq()?);
        handles.push(handle);
    }
    accepted_sequences.sort_unstable();
    assert_eq!(accepted_sequences, vec![42, 43, 44]);
    // Handlers and responses may run out of order. All CSeq values must have
    // been admitted in receive order before those independent tasks started.
    for handle in handles.into_iter().rev() {
        handle
            .start_response(StatusCode::OK, None, None)?
            .wait_sent()
            .await?;
    }
    for _ in 42..=44 {
        response(&peer, StatusCode::OK).await?;
    }
    let mut stale = make(Method::Info, 43)?;
    stale.headers.unique_push(Header::Via(
        format!(
            "SIP/2.0/UDP {};branch=z9hG4bK-stale;rport",
            peer.local_addr()?
        )
        .into(),
    ));
    peer.send_to(stale.to_string().as_bytes(), node.local)
        .await?;
    response(&peer, StatusCode::ServerInternalError).await?;
    let first = make(Method::Invite, 45)?;
    peer.send_to(first.to_string().as_bytes(), node.local)
        .await?;
    let (_, handle) = tokio::time::timeout(DEADLINE, node.admitted.recv())
        .await?
        .ok_or("no re-INVITE")?;
    handle
        .start_response(StatusCode::OK, None, None)?
        .wait_sent()
        .await?;
    let accepted = response(&peer, StatusCode::OK).await?;
    let ack = node.endpoint.make_ack(&first, &accepted)?;
    peer.send_to(ack.to_string().as_bytes(), node.local).await?;
    peer.send_to(make(Method::Invite, 46)?.to_string().as_bytes(), node.local)
        .await?;
    let (_, next) = tokio::time::timeout(DEADLINE, node.admitted.recv())
        .await?
        .ok_or("no next re-INVITE")?;
    assert_eq!(
        dialog
            .last_remote_ack()
            .ok_or("no ACK")?
            .cseq_header()?
            .seq()?,
        45
    );
    next.start_response(StatusCode::NotAcceptableHere, None, None)?
        .wait_sent()
        .await?;
    assert_eq!(dialog.snapshot().remote_cseq, 46);
    Ok(())
}

//! Exercise owned-operation contracts through real UDP and public API.
#![cfg(feature = "platform-tokio")]
use rsipstack::{
    dialog::v2managed::{
        authenticate::Credential,
        dialog::{Dialog, DialogSnapshot, DialogSnapshotState},
        dialog_layer::DialogLayer,
        invite_dialog::InviteDialog,
        operation::{AdmissionError, OperationError, ResponseOrigin},
        persistence::{
            DialogCheckpoint, DialogCheckpointReason as Reason, DialogPersistence,
            DialogPersistenceConfig, PersistenceFailure,
        },
        reinvite::ReinviteOptions,
        snapshot::DialogSnapshotRuntime,
        DialogId,
    },
    sip::{Header, HeadersExt, Method, Request, SipMessage, StatusCode, ToTypedHeader},
    transaction::{
        endpoint::{EndpointInnerRef, EndpointOption},
        key::TransactionRole,
        EndpointBuilder,
    },
    transport::{udp::UdpConnection, TransportLayer},
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Call {
    layer: DialogLayer,
    dialog: InviteDialog,
    peer: UdpSocket,
    endpoint: EndpointInnerRef,
    stop: CancellationToken,
}
impl Drop for Call {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Call {
    async fn new() -> Result<Self> {
        Self::configured(None, None).await
    }
    async fn configured(
        storage: Option<Arc<dyn DialogPersistence>>,
        credential: Option<Credential>,
    ) -> Result<Self> {
        let stop = CancellationToken::new();
        let peer = UdpSocket::bind("127.0.0.1:0").await?;
        let transport = TransportLayer::new(stop.clone());
        let udp =
            UdpConnection::create_connection("127.0.0.1:0".parse()?, None, Some(stop.clone()))
                .await?;
        let local = udp.get_addr().get_socketaddr()?;
        transport.add_transport(udp.into());
        let mut option = EndpointOption::default();
        option.t1 = Duration::from_millis(10);
        option.t1x64 = Duration::from_millis(200);
        let endpoint = Arc::new(
            EndpointBuilder::new()
                .with_transport_layer(transport)
                .with_option(option)
                .with_cancel_token(stop.clone())
                .build(),
        );
        let layer = DialogLayer::new(endpoint.inner.clone());
        if let Some(storage) = storage {
            layer.set_persistence(Some(DialogPersistenceConfig::new(storage)))?;
        }
        let id = DialogId {
            call_id: format!("op-{local}"),
            local_tag: "local".into(),
            remote_tag: "peer".into(),
        };
        let target = format!("sip:peer@{}", peer.local_addr()?);
        let snapshot = DialogSnapshot::builder(
            id.clone(),
            TransactionRole::Client,
            rsipstack::sip::From::from(format!("<sip:local@{local}>;tag=local")).typed()?,
            rsipstack::sip::To::from(format!("<{target}>;tag=peer")).typed()?,
            target.try_into()?,
        )
        .state(DialogSnapshotState::Confirmed)
        .local_cseq(1)
        .remote_cseq(0)
        .local_contact(Some(format!("sip:local@{local}").try_into()?))
        .runtime(DialogSnapshotRuntime::default())
        .build()?;
        let (states, _events) = layer.new_dialog_event_channel();
        assert!(matches!(
            layer.restore_from_snapshot_detailed(snapshot, states, credential)?,
            rsipstack::dialog::v2managed::RestoreOutcome::Restored(_)
        ));
        let Some(Dialog::Invite(dialog)) = layer.get_dialog(&id) else {
            panic!("missing restored dialog")
        };
        let serving = endpoint.clone();
        tokio::spawn(async move { serving.serve().await });
        Ok(Self {
            layer,
            dialog,
            peer,
            endpoint: endpoint.inner.clone(),
            stop,
        })
    }
    async fn receive(&self) -> Result<(Request, SocketAddr)> {
        let mut bytes = [0; 8192];
        let (len, source) =
            tokio::time::timeout(Duration::from_secs(2), self.peer.recv_from(&mut bytes)).await??;
        let SipMessage::Request(request) = std::str::from_utf8(&bytes[..len])?.try_into()? else {
            panic!("expected request")
        };
        Ok((request, source))
    }
    async fn answer(&self, request: &Request, source: SocketAddr, status: StatusCode) -> Result {
        let mut response = self.endpoint.make_response(request, status, None);
        if request.method == Method::Invite {
            response.headers.unique_push(Header::Contact(
                format!("<sip:peer@{}>", self.peer.local_addr()?).into(),
            ));
        }
        self.peer
            .send_to(response.to_string().as_bytes(), source)
            .await?;
        Ok(())
    }
}
struct Storage {
    reason: Reason,
    entered: CancellationToken,
    release: CancellationToken,
    gated: AtomicBool,
    fail: AtomicBool,
    checkpoints: Mutex<Vec<DialogCheckpoint>>,
}
impl Storage {
    fn new(reason: Reason) -> Arc<Self> {
        Arc::new(Self {
            reason,
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            gated: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            checkpoints: Mutex::new(vec![]),
        })
    }
}
#[async_trait::async_trait]
impl DialogPersistence for Storage {
    async fn save(&self, checkpoint: DialogCheckpoint) -> rsipstack::Result<()> {
        let gate = checkpoint.reason == self.reason && !self.gated.swap(true, Ordering::SeqCst);
        self.checkpoints.lock().unwrap().push(checkpoint);
        if gate {
            self.entered.cancel();
            self.release.cancelled().await;
            if self.fail.load(Ordering::SeqCst) {
                return Err(rsipstack::Error::Error(
                    "storage rejected checkpoint".into(),
                ));
            }
        }
        Ok(())
    }
}
#[tokio::test]
async fn checkpoint_receives_final_cseq_and_blocks_wire_without_reserving_later_cseqs() -> Result {
    let store = Storage::new(Reason::OutgoingRequest);
    let call = Call::configured(Some(store.clone()), None).await?;
    let first = call
        .dialog
        .start_request(Method::Info, None, Some(b"first".to_vec()))?;
    let second = call
        .dialog
        .start_request(Method::Info, None, Some(b"second".to_vec()))?;
    tokio::time::timeout(Duration::from_secs(2), store.entered.cancelled()).await?;
    assert_eq!(call.dialog.snapshot().local_cseq, 2);
    assert!(second.attempts().is_empty());
    let checkpoint = store.checkpoints.lock().unwrap()[0].clone();
    let Some(SipMessage::Request(stored)) = checkpoint.message else {
        panic!("missing request")
    };
    assert_eq!(stored.cseq_header()?.seq()?, 2);
    assert_eq!(checkpoint.snapshot.local_cseq, 2);
    let mut bytes = [0; 1024];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), call.peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    store.release.cancel();
    for expected in 2..=3 {
        let (request, source) = call.receive().await?;
        assert_eq!(request.cseq_header()?.seq()?, expected);
        assert!(store
            .checkpoints
            .lock()
            .unwrap()
            .iter()
            .any(|c| matches!(&c.message,
            Some(SipMessage::Request(r)) if r.cseq_header().unwrap().seq().unwrap() == expected)));
        call.answer(&request, source, StatusCode::OK).await?;
    }
    for operation in [first, second] {
        let result = operation.wait().await;
        assert!(result.signaling_result().is_ok());
        assert!(result.attempts()[0].checkpoint_committed());
        assert!(result.attempts()[0].transport_accepted());
    }
    call.dialog.persistence_barrier().wait().await?;
    assert!(call.dialog.snapshot().is_quiescent());
    Ok(())
}
#[tokio::test]
async fn dropped_waiter_keeps_sending_and_parallel_requests_preserve_first_send_order() -> Result {
    let call = Call::new().await?;
    let mut operations = Vec::new();
    for _ in 0..12 {
        operations.push(call.dialog.start_request(Method::Info, None, None)?);
    }
    let observer = operations[0].clone();
    let waiter = tokio::spawn(async move { observer.wait().await });
    waiter.abort();
    let mut requests = Vec::new();
    for seq in 2..14 {
        let (request, source) = call.receive().await?;
        assert_eq!(request.cseq_header()?.seq()?, seq);
        requests.push((request, source));
    }
    for (request, source) in requests.into_iter().rev() {
        call.answer(&request, source, StatusCode::BusyHere).await?;
    }
    for operation in operations {
        let result = operation.wait().await;
        assert_eq!(result.response().unwrap().status_code, StatusCode::BusyHere);
        assert!(
            result.signaling_result().is_ok(),
            "SIP rejection is separate from driver failure"
        );
    }
    assert!(call.dialog.snapshot().is_quiescent());
    Ok(())
}
#[tokio::test]
async fn late_reinvite_success_is_preserved_without_reviving_a_terminated_dialog() -> Result {
    let call = Call::new().await?;
    let invite =
        call.dialog
            .start_reinvite(None, Some(b"offer".to_vec()), ReinviteOptions::default())?;
    let (request, source) = call.receive().await?;
    assert!(matches!(
        call.dialog
            .start_reinvite(None, Some(b"other".to_vec()), ReinviteOptions::default()),
        Err(AdmissionError::NegotiationInProgress)
    ));
    let bye = call.dialog.start_request(Method::Bye, None, None)?;
    let (bye_request, bye_source) = call.receive().await?;
    assert_eq!(bye_request.method, Method::Bye);
    call.answer(&bye_request, bye_source, StatusCode::OK)
        .await?;
    assert!(bye.wait().await.dialog_state().is_terminated());
    call.answer(&request, source, StatusCode::OK).await?;
    let (ack, _) = call.receive().await?;
    assert_eq!(ack.method, Method::Ack);
    let result = invite.wait().await;
    assert_eq!(result.response().unwrap().status_code, StatusCode::OK);
    assert!(result.attempts()[0].ack_sent());
    assert!(result.dialog_state().is_terminated());
    assert!(matches!(
        call.dialog.start_request(Method::Info, None, None),
        Err(AdmissionError::NotConfirmed)
    ));
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_bye_commits_terminal_snapshot_without_relocking_identity() -> Result {
    let store = Storage::new(Reason::Terminated);
    store.release.cancel();
    let call = Call::configured(Some(store.clone()), None).await?;
    let bye = call.dialog.start_request(Method::Bye, None, None)?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.method, Method::Bye);
    call.answer(&request, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(Duration::from_secs(2), bye.wait()).await?;
    assert!(result.dialog_state().is_terminated());
    assert!(result.signaling_result().is_ok());
    call.dialog.persistence_barrier().wait().await?;
    let records = store.checkpoints.lock().unwrap();
    assert!(records.iter().any(|c| c.reason == Reason::Terminated));
    let last = records.last().unwrap();
    assert_eq!(last.snapshot.runtime.pending.operations, 0);
    assert_eq!(last.snapshot.state, DialogSnapshotState::Terminated);
    Ok(())
}

#[tokio::test]
async fn late_answer_cleanup_requires_its_receipt_and_does_not_revive_the_session() -> Result {
    let call = Call::new().await?;
    let invite =
        call.dialog
            .start_reinvite(None, Some(b"offer".to_vec()), ReinviteOptions::default())?;
    let (request, source) = call.receive().await?;
    let bye = call.dialog.start_request(Method::Bye, None, None)?;
    let (end, remote) = call.receive().await?;
    call.answer(&end, remote, StatusCode::OK).await?;
    assert!(bye.wait().await.dialog_state().is_terminated());
    call.answer(&request, source, StatusCode::OK).await?;
    assert_eq!(call.receive().await?.0.method, Method::Ack);
    let result = invite.wait().await;
    let foreign = Call::new().await?;
    assert!(foreign
        .dialog
        .start_late_answer_cleanup(&result, None)
        .is_err());
    let cleanup = call.dialog.start_late_answer_cleanup(&result, None)?;
    assert!(call
        .dialog
        .start_late_answer_cleanup(&result.clone(), None)
        .is_err());
    let (cleanup_request, remote) = call.receive().await?;
    assert_eq!(cleanup_request.method, Method::Bye);
    assert_eq!(
        cleanup_request.cseq_header()?.seq()?,
        end.cseq_header()?.seq()? + 1
    );
    call.answer(&cleanup_request, remote, StatusCode::OK)
        .await?;
    assert!(cleanup.wait().await.dialog_state().is_terminated());
    assert!(call.dialog.start_request(Method::Info, None, None).is_err());
    Ok(())
}

#[tokio::test]
async fn transaction_timeout_is_distinct_from_a_peer_408() -> Result {
    let call = Call::new().await?;
    let local = call.dialog.start_request(Method::Options, None, None)?;
    let result = tokio::time::timeout(Duration::from_secs(2), local.wait()).await?;
    assert_eq!(result.response_origin(), Some(ResponseOrigin::Local));
    assert!(matches!(
        result.signaling_result(),
        Err(OperationError::Timeout)
    ));
    // Drain retries of the timed-out operation before sending another one.
    let mut buffer = [0; 8192];
    while call.peer.try_recv(&mut buffer).is_ok() {}
    let peer = call.dialog.start_request(Method::Options, None, None)?;
    let (request, source) = call.receive().await?;
    call.answer(&request, source, StatusCode::RequestTimeout)
        .await?;
    let result = peer.wait().await;
    assert_eq!(result.response_origin(), Some(ResponseOrigin::Peer));
    assert!(result.signaling_result().is_ok());
    Ok(())
}

#[tokio::test]
async fn failed_pre_ack_checkpoint_retains_the_peer_response_and_blocks_future_sends() -> Result {
    let store = Storage::new(Reason::BeforeAck);
    store.fail.store(true, Ordering::SeqCst);
    store.release.cancel();
    let call = Call::configured(Some(store.clone()), None).await?;
    let operation =
        call.dialog
            .start_reinvite(None, Some(b"offer".to_vec()), ReinviteOptions::default())?;
    let (request, source) = call.receive().await?;
    call.answer(&request, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(Duration::from_secs(2), operation.wait()).await?;
    assert_eq!(result.response().unwrap().status_code, StatusCode::OK);
    assert_eq!(result.response_origin(), Some(ResponseOrigin::Peer));
    assert!(matches!(
        result.signaling_result(),
        Err(OperationError::Persistence(PersistenceFailure::Storage(_)))
    ));
    assert!(!result.attempts()[0].ack_sent());
    assert!(matches!(
        call.dialog.start_request(Method::Info, None, None),
        Err(AdmissionError::Persistence(_))
    ));
    assert!(call.dialog.persistence_barrier().wait().await.is_err());
    let mut bytes = [0; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), call.peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn authentication_retries_share_the_send_queue_and_preserve_attempt_responses() -> Result {
    let store = Storage::new(Reason::OutgoingRequest);
    store.release.cancel();
    let call = Call::configured(
        Some(store.clone()),
        Some(Credential {
            username: "user".into(),
            password: "password".into(),
            ..Default::default()
        }),
    )
    .await?;
    let first = call.dialog.start_request(Method::Info, None, None)?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.cseq_header()?.seq()?, 2);
    let second = call.dialog.start_request(Method::Info, None, None)?;
    let mut response = call
        .endpoint
        .make_response(&request, StatusCode::Unauthorized, None);
    response.headers.push(Header::WwwAuthenticate(
        "Digest realm=\"test\", nonce=\"nonce\", algorithm=MD5".into(),
    ));
    call.peer
        .send_to(response.to_string().as_bytes(), source)
        .await?;
    for expected in 3..=4 {
        let (request, source) = call.receive().await?;
        assert_eq!(request.cseq_header()?.seq()?, expected);
        assert_eq!(request.authorization_header().is_some(), expected == 4);
        call.answer(&request, source, StatusCode::OK).await?;
    }
    let completed = first.wait().await;
    assert!(completed.signaling_result().is_ok());
    assert_eq!(completed.attempts().len(), 2);
    assert_eq!(
        completed.attempts()[0].response().unwrap().status_code,
        StatusCode::Unauthorized
    );
    assert_eq!(completed.response().unwrap().status_code, StatusCode::OK);
    assert!(completed
        .attempts()
        .iter()
        .all(|a| a.checkpoint_committed()));
    assert!(second.wait().await.signaling_result().is_ok());
    Ok(())
}

#[tokio::test]
async fn cancellation_before_send_does_not_consume_cseq() -> Result {
    let call = Call::new().await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let operation = call.dialog.start_reinvite(
        None,
        Some(b"offer".to_vec()),
        ReinviteOptions {
            cancellation: cancel,
            ..Default::default()
        },
    )?;
    let result = operation.wait().await;
    assert!(matches!(
        result.signaling_result(),
        Err(OperationError::CancelledBeforeSend)
    ));
    assert!(result.attempts().is_empty());
    assert_eq!(call.dialog.snapshot().local_cseq, 1);
    assert!(call.dialog.snapshot().is_quiescent());
    Ok(())
}

#[tokio::test]
async fn interrupted_recovery_only_sends_fresh_bye_and_retains_evidence() -> Result {
    let call = Call::new().await?;
    let layer = DialogLayer::new(call.endpoint.clone());
    let storage = Storage::new(Reason::OutgoingRequest);
    storage.release.cancel();
    layer.set_persistence(Some(DialogPersistenceConfig::new(storage.clone())))?;
    let mut snapshot = call.dialog.snapshot();
    snapshot.runtime.pending.local_invite = true;
    snapshot.runtime.pending.operations = 3;
    snapshot.runtime.pending.negotiations = 1;
    snapshot.local_cseq = 42;
    let (states, _) = layer.new_dialog_event_channel();
    assert!(matches!(
        layer.restore_from_snapshot_detailed(snapshot.clone(), states.clone(), None)?,
        rsipstack::dialog::v2managed::recovery::RestoreOutcome::RecoveryRequired(_)
    ));
    let mut missing_initial_ack = snapshot.clone();
    missing_initial_ack.runtime.pending.initial_ack = true;
    assert!(layer
        .start_recovery_termination(missing_initial_ack, states.clone(), None, None)
        .is_err());
    let operation =
        layer.start_recovery_termination(snapshot.clone(), states.clone(), None, None)?;
    assert!(layer
        .start_recovery_termination(snapshot.clone(), states.clone(), None, None)
        .is_err());
    assert!(layer
        .restore_from_snapshot_detailed(snapshot.clone(), states, None)
        .is_err());
    assert!(layer.get_dialog(&snapshot.id).is_none());
    let (bye, source) = call.receive().await?;
    assert_eq!(bye.method, Method::Bye);
    assert_eq!(bye.cseq_header()?.seq()?, 43);
    assert_eq!(bye.uri, snapshot.remote_uri);
    call.answer(&bye, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(Duration::from_secs(2), operation.wait()).await?;
    assert!(result.signaling_result().is_ok());
    assert!(result.dialog_state().is_terminated());
    let records = storage.checkpoints.lock().unwrap();
    assert!(!records.is_empty());
    for record in records.iter() {
        assert!(record.snapshot.runtime.pending.local_invite);
        assert!(record.snapshot.runtime.pending.operations >= 3);
        assert_eq!(record.snapshot.runtime.pending.negotiations, 1);
    }
    assert_eq!(
        records.last().unwrap().snapshot.state,
        DialogSnapshotState::Terminated
    );
    assert!(layer.get_dialog(&snapshot.id).is_none());
    Ok(())
}

struct MissingAnswer;
#[async_trait::async_trait]
impl rsipstack::dialog::v2managed::reinvite::InviteAnswerHandler for MissingAnswer {
    async fn answer(
        &self,
        _: rsipstack::sip::Response,
    ) -> rsipstack::Result<rsipstack::dialog::v2managed::reinvite::AckBody> {
        Err(rsipstack::Error::Error(
            "application ended before answering".into(),
        ))
    }
    fn fallback_answer(
        &self,
        response: &rsipstack::sip::Response,
    ) -> rsipstack::dialog::v2managed::reinvite::AckBody {
        rsipstack::dialog::v2managed::reinvite::AckBody::sdp(
            String::from_utf8_lossy(&response.body)
                .replace("m=audio 9000", "m=audio 0")
                .into_bytes(),
        )
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_offerless_answer_acks_then_terminates_with_persistence_enabled() -> Result {
    let storage = Storage::new(Reason::Terminated);
    storage.release.cancel();
    let call = Call::configured(Some(storage.clone()), None).await?;
    let operation = call.dialog.start_reinvite(
        None,
        None,
        ReinviteOptions {
            answer_handler: Some(Arc::new(MissingAnswer)),
            ..Default::default()
        },
    )?;
    let (invite, source) = call.receive().await?;
    let mut response = call.endpoint.make_response(&invite, StatusCode::OK, None);
    response.body =
        b"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=test\r\nt=0 0\r\nm=audio 9000 RTP/AVP 0\r\n"
            .to_vec();
    response.headers.unique_push(Header::Contact(
        format!("<sip:peer@{}>", call.peer.local_addr()?).into(),
    ));
    response
        .headers
        .unique_push(Header::ContentType("application/sdp".into()));
    response
        .headers
        .unique_push(Header::ContentLength((response.body.len() as u32).into()));
    call.peer
        .send_to(response.to_string().as_bytes(), source)
        .await?;
    assert_eq!(call.receive().await?.0.method, Method::Ack);
    let (bye, source) = call.receive().await?;
    assert_eq!(bye.method, Method::Bye);
    call.answer(&bye, source, StatusCode::OK).await?;
    let result = tokio::time::timeout(Duration::from_secs(2), operation.wait()).await?;
    assert!(result.signaling_result().is_err());
    assert_eq!(result.response().unwrap().status_code, StatusCode::OK);
    assert!(result.dialog_state().is_terminated());
    Ok(())
}

#[tokio::test]
async fn managed_drain_preserves_dropped_work_and_keeps_quiet_calls_alive() -> Result {
    let call = Call::new().await?;
    let operation = call.dialog.start_request(Method::Info, None, None)?;
    drop(operation);
    let (request, source) = call.receive().await?;
    call.layer.close_admission();
    assert!(matches!(
        call.dialog.start_request(Method::Options, None, None),
        Err(AdmissionError::LayerClosed)
    ));
    let report = call.layer.drain(Duration::from_millis(20)).await;
    assert!(report.timed_out);
    assert!(!report.is_drained());
    assert_eq!(report.remaining.operations.len(), 1);
    assert!(
        call.dialog
            .diagnostics()
            .snapshot
            .runtime
            .pending
            .operations
            > 0
    );
    call.answer(&request, source, StatusCode::OK).await?;
    let report = call.layer.drain(Duration::from_secs(2)).await;
    assert!(report.is_drained(), "{report:?}");
    assert!(report
        .persistence
        .iter()
        .all(|p| matches!(p.result, Some(Ok(())))));
    assert!(call.dialog.state().is_confirmed());
    assert_eq!(call.layer.len(), 1);
    // Explicit cleanup is still allowed after the normal admission gate closes.
    let bye = call.dialog.start_termination(Default::default())?;
    loop {
        let (request, source) = call.receive().await?;
        call.answer(&request, source, StatusCode::OK).await?;
        if request.method == Method::Bye {
            break;
        }
        assert_eq!(request.method, Method::Info); // A previously queued UDP retransmission.
    }
    bye.wait().await.signaling_result().map_err(Clone::clone)?;
    assert!(call.layer.drain(Duration::from_secs(2)).await.is_drained());
    Ok(())
}

#[tokio::test]
async fn managed_body_has_one_content_type_and_moves_existing_bytes() -> Result {
    use rsipstack::dialog::v2managed::{Body, InviteOptions};
    let bytes = Vec::from(&b"v=0\r\n"[..]);
    let address = bytes.as_ptr();
    let body = Body::sdp(bytes);
    assert_eq!(address, body.as_bytes().as_ptr());
    let bytes = body.into_bytes();
    assert_eq!(address, bytes.as_ptr());
    let options: rsipstack::dialog::v2managed::InviteOption = InviteOptions::default()
        .body(Body::sdp("v=0\r\n"))
        .headers(vec![Header::ContentType("text/plain".into())])
        .into();
    assert_eq!(options.content_type.as_deref(), Some("application/sdp"));
    assert!(options.headers.unwrap().is_empty());
    let call = Call::new().await?;
    let operation = call.dialog.start_request_body(
        Method::Info,
        Some(vec![Header::ContentType("application/incorrect".into())]),
        Body::text("hello"),
    )?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.body, b"hello");
    assert_eq!(
        request
            .headers
            .iter()
            .filter(|h| h.name().eq_ignore_ascii_case("Content-Type"))
            .count(),
        1
    );
    assert!(request
        .headers
        .iter()
        .any(|h| matches!(h, Header::ContentType(value) if value.value() == "text/plain")));
    call.answer(&request, source, StatusCode::OK).await?;
    operation
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    Ok(())
}

#[tokio::test]
async fn managed_drain_waits_for_storage_and_reports_failure_separately() -> Result {
    let store = Storage::new(Reason::OutgoingRequest);
    let call = Call::configured(Some(store.clone()), None).await?;
    let operation = call.dialog.start_request(Method::Options, None, None)?;
    tokio::time::timeout(Duration::from_secs(2), store.entered.cancelled()).await?;
    let report = call.layer.drain(Duration::from_millis(20)).await;
    assert!(report.timed_out);
    assert!(!report.remaining.operations.is_empty());
    store.fail.store(true, Ordering::SeqCst);
    store.release.cancel();
    assert!(operation.wait().await.signaling_result().is_err());
    let report = call.layer.drain(Duration::from_secs(2)).await;
    assert!(report.is_drained(), "{report:?}");
    assert!(report
        .persistence
        .iter()
        .any(|p| matches!(p.result, Some(Err(_)))));
    assert!(call
        .dialog
        .diagnostics()
        .persistence
        .unwrap()
        .failure
        .is_some());
    Ok(())
}

#[tokio::test]
async fn managed_refer_helpers_use_owned_requests_and_correct_headers() -> Result {
    let call = Call::new().await?;
    let refer = call
        .dialog
        .start_refer("sip:transfer@example.test", None, None)?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.method, Method::Refer);
    assert!(request
        .headers
        .iter()
        .any(|h| matches!(h, Header::ReferTo(v) if v.value() == "sip:transfer@example.test")));
    call.answer(&request, source, StatusCode::Accepted).await?;
    refer
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    let notify = call
        .dialog
        .start_notify_refer(StatusCode::OK, "terminated;reason=noresource")?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.method, Method::Notify);
    assert_eq!(request.body, b"SIP/2.0 200 OK\r\n");
    assert!(request
        .headers
        .iter()
        .any(|h| matches!(h, Header::Event(v) if v.value() == "refer")));
    assert!(request.headers.iter().any(
        |h| matches!(h, Header::SubscriptionState(v) if v.value() == "terminated;reason=noresource")
    ));
    assert!(request
        .headers
        .iter()
        .any(|h| matches!(h, Header::ContentType(v) if v.value() == "message/sipfrag")));
    call.answer(&request, source, StatusCode::OK).await?;
    notify
        .wait()
        .await
        .signaling_result()
        .map_err(Clone::clone)?;
    call.layer.close_admission();
    assert!(matches!(
        call.dialog
            .start_refer("sip:transfer@example.test", None, None),
        Err(AdmissionError::LayerClosed)
    ));
    Ok(())
}

fn glare_policy(attempts: usize) -> rsipstack::dialog::v2managed::GlareRetryPolicy {
    rsipstack::dialog::v2managed::GlareRetryPolicy {
        max_attempts: std::num::NonZeroUsize::new(attempts).unwrap(),
    }
}

#[tokio::test]
async fn managed_glare_retry_owns_backoff_and_retains_every_response() -> Result {
    let call = Call::new().await?;
    let operation = call.dialog.start_reinvite_body_with_retry(
        None,
        rsipstack::dialog::v2managed::Body::sdp("v=0\r\n"),
        ReinviteOptions::default(),
        glare_policy(2),
    )?;
    let waiter = operation.clone();
    drop(operation);
    let (first, source) = call.receive().await?;
    assert_eq!(first.method, Method::Invite);
    call.answer(&first, source, StatusCode::RequestPending)
        .await?;
    let (ack, _) = call.receive().await?;
    assert_eq!(ack.method, Method::Ack);
    // The retry sequence was already admitted; drain cannot silently cancel it.
    call.layer.close_admission();
    let mut bytes = [0; 8192];
    let (len, source) =
        tokio::time::timeout(Duration::from_secs(5), call.peer.recv_from(&mut bytes)).await??;
    let SipMessage::Request(second) = SipMessage::try_from(&bytes[..len])? else {
        panic!("expected retry")
    };
    assert_eq!(second.method, Method::Invite);
    assert_eq!(
        second.cseq_header()?.seq()?,
        first.cseq_header()?.seq()? + 1
    );
    assert_ne!(second.via_header()?, first.via_header()?);
    call.answer(&second, source, StatusCode::NotAcceptableHere)
        .await?;
    let result = tokio::time::timeout(Duration::from_secs(2), waiter.wait()).await?;
    result.signaling_result().map_err(Clone::clone)?;
    assert_eq!(result.attempts().len(), 2);
    assert_eq!(
        result.attempts()[0].response().unwrap().status_code,
        StatusCode::RequestPending
    );
    assert_eq!(
        result.response().unwrap().status_code,
        StatusCode::NotAcceptableHere
    );
    assert!(call.layer.drain(Duration::from_secs(1)).await.is_drained());
    assert!(call.dialog.state().is_confirmed());
    Ok(())
}

#[tokio::test]
async fn managed_glare_limit_and_cancel_do_not_discard_491() -> Result {
    use rsipstack::dialog::v2managed::GlareRetryError;
    for cancel in [false, true] {
        let call = Call::new().await?;
        let operation = call.dialog.start_reinvite_body_with_retry(
            None,
            rsipstack::dialog::v2managed::Body::sdp("v=0\r\n"),
            ReinviteOptions::default(),
            glare_policy(if cancel { 3 } else { 1 }),
        )?;
        let (request, source) = call.receive().await?;
        call.answer(&request, source, StatusCode::RequestPending)
            .await?;
        assert_eq!(call.receive().await?.0.method, Method::Ack);
        if cancel {
            operation.request_cancellation();
        }
        let result = tokio::time::timeout(Duration::from_secs(1), operation.wait()).await?;
        assert_eq!(result.attempts().len(), 1);
        assert_eq!(
            result.response().unwrap().status_code,
            StatusCode::RequestPending
        );
        if cancel {
            assert!(matches!(
                result.signaling_result(),
                Err(GlareRetryError::Cancelled)
            ));
        } else {
            assert!(matches!(
                result.signaling_result(),
                Err(GlareRetryError::AttemptsExhausted)
            ));
        }
        assert!(call.layer.drain(Duration::from_secs(1)).await.is_drained());
    }
    Ok(())
}

#[tokio::test]
async fn managed_glare_backoff_stops_when_call_ends() -> Result {
    use rsipstack::dialog::v2managed::GlareRetryError;
    let call = Call::new().await?;
    let operation = call.dialog.start_reinvite_body_with_retry(
        None,
        rsipstack::dialog::v2managed::Body::sdp("v=0\r\n"),
        ReinviteOptions::default(),
        glare_policy(3),
    )?;
    let (request, source) = call.receive().await?;
    call.answer(&request, source, StatusCode::RequestPending)
        .await?;
    assert_eq!(call.receive().await?.0.method, Method::Ack);
    let bye = call.dialog.start_termination(Default::default())?;
    let (request, source) = call.receive().await?;
    assert_eq!(request.method, Method::Bye);
    call.answer(&request, source, StatusCode::OK).await?;
    bye.wait().await.signaling_result().map_err(Clone::clone)?;
    let result = tokio::time::timeout(Duration::from_secs(1), operation.wait()).await?;
    assert!(matches!(
        result.signaling_result(),
        Err(GlareRetryError::DialogEnded)
    ));
    assert_eq!(result.attempts().len(), 1);
    Ok(())
}

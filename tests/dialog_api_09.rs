//! Downstream reconstruction uses only public API; no mutable inner state.
#![cfg(feature = "platform-tokio")]
use rsipstack::{
    dialog::{
        dialog::{Dialog, DialogSnapshot, DialogSnapshotState},
        dialog_layer::DialogLayer,
        recovery::{RestoreError, RestoreOutcome},
        snapshot::DialogSnapshotRuntime,
        DialogId,
    },
    sip::ToTypedHeader,
    transaction::key::TransactionRole,
    EndpointBuilder,
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn saved() -> Result<DialogSnapshot> {
    Ok(DialogSnapshot::builder(
        DialogId {
            call_id: "recovery".into(),
            local_tag: "local".into(),
            remote_tag: "peer".into(),
        },
        TransactionRole::Client,
        rsipstack::sip::From::from("<sip:local@localhost>;tag=local").typed()?,
        rsipstack::sip::To::from("<sip:peer@localhost>;tag=peer").typed()?,
        "sip:peer@127.0.0.1:5061".try_into()?,
    )
    .state(DialogSnapshotState::Confirmed)
    .local_cseq(42)
    .remote_cseq(7)
    .runtime(DialogSnapshotRuntime::default())
    .build()?)
}
#[tokio::test]
async fn detailed_restore_retains_pending_work_and_legacy_meanings() -> Result {
    let layer = DialogLayer::new(EndpointBuilder::new().build().inner);
    let (states, _events) = layer.new_dialog_state_channel();
    let mut pending = saved()?;
    pending.runtime.pending.initial_ack = true;
    match layer.restore_from_snapshot_detailed(pending.clone(), states.clone(), None)? {
        RestoreOutcome::RecoveryRequired(record) => {
            assert!(record.runtime.pending.initial_ack);
            assert_eq!(record.local_cseq, 42);
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(layer.is_empty());
    assert!(layer
        .restore_from_snapshot(pending, states.clone())
        .is_err());
    let mut corrupt = saved()?;
    corrupt.runtime.revision = u64::MAX;
    assert!(matches!(
        layer.restore_from_snapshot_detailed(corrupt, states.clone(), None),
        Err(RestoreError::InvalidLifecycle(_))
    ));
    let mut early = saved()?;
    early.state = DialogSnapshotState::Early;
    assert!(matches!(
        layer.restore_from_snapshot_detailed(early.clone(), states.clone(), None)?,
        RestoreOutcome::SkippedNonConfirmed(DialogSnapshotState::Early)
    ));
    assert!(!layer.restore_from_snapshot(early, states.clone())?);
    let snapshot = saved()?;
    assert!(matches!(
        layer.restore_from_snapshot_detailed(snapshot.clone(), states.clone(), None)?,
        RestoreOutcome::Restored(_)
    ));
    assert!(matches!(
        layer.restore_from_snapshot_detailed(snapshot.clone(), states.clone(), None)?,
        RestoreOutcome::AlreadyPresent(_)
    ));
    assert!(!layer.restore_from_snapshot(snapshot.clone(), states)?);
    let Some(Dialog::Invite(dialog)) = layer.get_dialog(&snapshot.id) else {
        panic!("missing dialog")
    };
    dialog.persistence_barrier().wait().await?;
    assert!(dialog.persistence_status().is_none());
    assert_eq!(dialog.snapshot().local_cseq, 42);
    Ok(())
}

#[tokio::test]
async fn legacy_raw_state_remains_available_without_aliasing_owned_layer_state() -> Result {
    use rsipstack::dialog::{
        dialog::{DialogInner, DialogState, TerminatedReason},
        invite_dialog::InviteDialog,
        operation::AdmissionError,
    };
    use rsipstack::sip::{Method, Request, Response};
    use rsipstack::transaction::transaction::transaction_event_sender_noop;
    use std::sync::{atomic::Ordering, Arc};
    let endpoint = EndpointBuilder::new().build();
    let layer = DialogLayer::new(endpoint.inner.clone());
    let (states, _events) = layer.new_dialog_state_channel();
    let request: Request = "INVITE sip:peer@127.0.0.1:5061 SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-legacy\r\nFrom: <sip:local@localhost>;tag=local\r\nTo: <sip:peer@localhost>;tag=peer\r\nCall-ID: legacy\r\nCSeq: 1 INVITE\r\nContact: <sip:local@127.0.0.1:5060>\r\nContent-Length: 0\r\n\r\n".try_into()?;
    let id = DialogId {
        call_id: "legacy".into(),
        local_tag: "local".into(),
        remote_tag: "peer".into(),
    };
    let raw = Arc::new(DialogInner::new(
        TransactionRole::Client,
        id.clone(),
        request,
        endpoint.inner.clone(),
        states.clone(),
        None,
        None,
        transaction_event_sender_noop(),
    )?);
    raw.local_seq.store(41, Ordering::SeqCst);
    *raw.state.lock() = DialogState::Confirmed(id.clone(), Response::default());
    let legacy = InviteDialog::from_inner(raw.clone());
    assert!(matches!(
        legacy.start_request(Method::Info, None, None),
        Err(AdmissionError::ExternallyOwnedState)
    ));
    // Existing methods still compile and retain their lazy-future API.
    drop(legacy.info(None, None));
    assert!(layer.restore_from_snapshot(raw.snapshot(), states)?);
    raw.local_seq.store(999, Ordering::SeqCst);
    *raw.state.lock() = DialogState::Terminated(id.clone(), TerminatedReason::UacBye);
    let Some(Dialog::Invite(owned)) = layer.get_dialog(&id) else {
        panic!("missing dialog")
    };
    assert!(owned.state().is_confirmed());
    assert_eq!(owned.snapshot().local_cseq, 41);
    assert!(legacy.state().is_terminated());
    Ok(())
}

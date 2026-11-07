use super::{bounded, log_dir, Sipp, Stack};
use crate::interop::{Required, TestResult};
use rsipstack::{
    dialog::v2managed::{
        subscription_event_channel, SubscriptionDialog, SubscriptionEventType, SubscriptionLayer,
        SubscriptionOptions, SubscriptionPhase,
    },
    sip::{Method, StatusCode},
};
use std::time::Duration;

async fn receive(stack: &mut Stack, layer: &SubscriptionLayer) -> TestResult<SubscriptionDialog> {
    let transaction = bounded("managed NOTIFY", stack.event_transactions.recv())
        .await?
        .required()?;
    let incoming = layer.receive_notify(transaction).required()?;
    let dialog = incoming.dialog().clone();
    bounded(
        "NOTIFY response",
        incoming
            .start_response(StatusCode::OK, None)
            .required()?
            .wait_sent(),
    )
    .await?
    .required()?;
    Ok(dialog)
}
async fn scenario(name: &str, transport: &str) -> TestResult {
    let dir = log_dir(name)?;
    let mut stack = Stack::start_events(transport, &dir).await?;
    let peer = Sipp::start("managed_subscription", transport, &dir, &stack.uri).await?;
    let layer = SubscriptionLayer::new(stack.layer.endpoint.clone());
    let (events, _) = subscription_event_channel();
    let (attempt, initial) = layer
        .start_subscription(
            SubscriptionOptions::new(
                stack.uri.clone(),
                peer.uri.clone(),
                stack.uri.clone(),
                SubscriptionEventType::new("presence").required()?,
                60,
            ),
            events,
        )
        .required()?;
    let dialog = receive(&mut stack, &layer).await?;
    bounded("initial SUBSCRIBE", initial.wait())
        .await?
        .signaling_result()
        .required()?;
    check_eq!(attempt.branches().len(), 1);
    check_eq!(dialog.status().phase, SubscriptionPhase::Active);
    let refresh = dialog.start_refresh(30, None).required()?;
    receive(&mut stack, &layer).await?;
    bounded("refresh SUBSCRIBE", refresh.wait())
        .await?
        .signaling_result()
        .required()?;
    let unsubscribe = dialog.start_unsubscribe().required()?;
    receive(&mut stack, &layer).await?;
    bounded("unsubscribe", unsubscribe.wait())
        .await?
        .signaling_result()
        .required()?;
    peer.finish().await?;
    check!(matches!(
        dialog.status().phase,
        SubscriptionPhase::Terminated(_)
    ));
    check!(layer.dialogs().is_empty());
    check!(layer.drain(Duration::from_secs(1)).await.is_drained());
    check_eq!(stack.wire.requests(Method::Subscribe), 3);
    check_eq!(stack.wire.requests(Method::Notify), 3);
    check_eq!(stack.wire.requests(Method::Ack), 0);
    stack
        .finish(
            &dir,
            format!("{name}: early NOTIFY, refresh and terminal NOTIFY over {transport}\n"),
        )
        .await?;
    Ok(())
}
#[tokio::test]
#[ignore = "requires SIPp; use scripts/interop/run.sh sipp managed_subscription"]
async fn udp_managed_subscription() -> TestResult {
    scenario("udp_managed_subscription", "udp").await
}
#[tokio::test]
#[ignore = "requires SIPp; use scripts/interop/run.sh sipp managed_subscription"]
async fn tcp_managed_subscription() -> TestResult {
    scenario("tcp_managed_subscription", "tcp").await
}

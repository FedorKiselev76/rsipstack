use super::{bounded, log_dir, Sipp, Stack};
use crate::interop::{Required, TestResult};
use rsipstack::{
    dialog::v2managed::{Body, DialogLayer, PublicationOptions},
    sip::Method,
};
use std::time::Duration;

async fn scenario(name: &str, transport: &str) -> TestResult {
    let dir = log_dir(name)?;
    let stack = Stack::start(transport, &dir).await?;
    let peer = Sipp::start("publication", transport, &dir, &stack.uri).await?;
    let layer = DialogLayer::new(stack.layer.endpoint.clone());
    let publication = layer
        .publication(PublicationOptions::new(
            stack.uri.clone(),
            peer.uri.clone(),
            "presence",
        ))
        .required()?;
    for stage in 0..4 {
        let operation = match stage {
            0 | 2 => {
                publication.start_publish(Body::new("application/pidf+xml", "<presence/>"), 60)
            }
            1 => publication.start_refresh(60),
            _ => publication.start_remove(),
        }
        .required()?;
        let completion = bounded("managed PUBLISH", operation.wait()).await?;
        completion.signaling_result().required()?;
        check_eq!(completion.response().required()?.status_code.code(), 200);
        if stage < 3 {
            check_eq!(publication.status().etag, Some(format!("v{}", stage + 1)));
        }
    }
    check!(publication.status().etag.is_none());
    check!(layer.drain(Duration::from_secs(1)).await.is_drained());
    peer.finish().await?;
    check_eq!(stack.wire.requests(Method::Publish), 4);
    check_eq!(stack.wire.requests(Method::Ack), 0);
    stack
        .finish(
            &dir,
            format!(
                "{name}: managed publication create, refresh, modify and remove over {transport}\n"
            ),
        )
        .await?;
    Ok(())
}
#[tokio::test]
#[ignore = "requires SIPp; use scripts/interop/run.sh sipp publication"]
async fn udp_managed_publication() -> TestResult {
    scenario("udp_managed_publication", "udp").await
}
#[tokio::test]
#[ignore = "requires SIPp; use scripts/interop/run.sh sipp publication"]
async fn tcp_managed_publication() -> TestResult {
    scenario("tcp_managed_publication", "tcp").await
}

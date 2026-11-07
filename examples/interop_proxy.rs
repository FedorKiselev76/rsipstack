//! Single-target proxy interoperability application using the public proxy API.
mod interop_support;
use interop_support::{App, AppResult};
use rsipstack::{
    proxy::v2managed::{ProxyConfig, ProxyLayer, ProxyTarget},
    sip::{HeadersExt, Method},
};

#[tokio::main]
async fn main() -> AppResult {
    let app = App::start(true).await?;
    let proxy = ProxyLayer::new(
        app.endpoint.inner.clone(),
        ProxyConfig::new(app.uri.clone())?,
    );
    let mut incoming = app.endpoint.incoming_transactions()?;
    let target = app.config.target.clone();
    let calls = app.calls.clone();
    let tasks = app.tasks.clone();
    app.tasks.spawn("proxy router", async move {
        while let Some(tx) = incoming.recv().await {
            let initial =
                tx.original.method == Method::Invite && tx.original.to_header()?.tag()?.is_none();
            let method = tx.original.method.clone();
            let id = tx.original.call_id_header()?.value().to_owned();
            let route = if initial {
                ProxyTarget::next_hop(target.clone()).with_request_uri(target.clone())
            } else {
                ProxyTarget::routed()
            };
            if initial {
                calls.add(id.clone());
                tasks.event(format!("CALL {id}"));
            }
            let operation = proxy.forward(tx, route);
            let calls = calls.clone();
            let events = tasks.clone();
            tasks.spawn("proxy response observer", async move {
                if let Some(response) = operation.wait_final_response().await? {
                    if (initial && response.status_code.code() >= 300)
                        || (method == Method::Bye && response.status_code.code() >= 200)
                    {
                        calls.remove(&id);
                        events.event(format!("ENDED {id}"));
                    }
                }
                // The stack retains forwarding through transaction retirement,
                // including repeated/forked successes after this observer drops.
                Ok(())
            });
        }
        Ok(())
    });
    app.run(|| 0).await
}

# Managed dialog API

`rsipstack::dialog::v2managed` is an experimental, owned INVITE-dialog API.
The existing `rsipstack::dialog` paths remain available. Both interfaces are
compiled normally, including without default Cargo features. Handles cannot be
converted between the two generations. Registration remains a separate API.

## Migration map

| Task | Existing interface | Managed interface |
| --- | --- | --- |
| Outgoing call | `do_invite` / `do_invite_async` | `start_invite`, then observe `InitialInviteOperation` |
| Incoming call | `get_or_create_server_invite`, drive `handle` | `start_incoming_invite`, then observe `IncomingInvite` |
| Ring / accept / reject | `ringing` / `accept` / `reject` | `start_response`, observe send and ACK receipts |
| Terminate at any call phase | Application selects CANCEL, reject or BYE | `start_termination` |
| Confirmed request | `info` / `options` / `message` / `request` | `start_request`, observe response and signaling separately |
| SDP body | Separate bytes and Content-Type | `Body::sdp`, `start_reinvite_body` / `start_response_body` |
| re-INVITE | `prepare_reinvite`, `send_reinvite` | `start_reinvite`; allocation and send are stack-owned |
| Bridge renegotiation | Existing `ReinviteBridge` | `v2managed::ReinviteBridge` with managed handles |
| Incoming requests and progress | Mixed `DialogState` receiver | Ordered `DialogEvent::{Notification, IncomingRequest}` |
| Recovery | Boolean restore wrapper | `restore_from_snapshot_detailed`, inspect every outcome |
| Live state inspection | Internal fields / mixed state | `state`, `snapshot`, `diagnostics` (owned data only) |
| Stop accepting commands | Application-owned coordination | `close_admission`, then `drain` with a time budget |
| Event subscriptions | Borrowed subscription dialogs | `SubscriptionLayer`, owned exchanges and response receipts |
| Event publication | Borrowed publication dialog | `Publication`, ETags and serialized exchanges |
| Proxy | `rsipstack::proxy::ProxyLayer` | `rsipstack::proxy::v2managed::ProxyLayer` (same owned implementation) |

Standalone `SUBSCRIBE`/`NOTIFY` usages use `SubscriptionLayer` and `SubscriptionDialog`.
Event publication uses the separate managed `Publication` type. An in-dialog `REFER` or `NOTIFY` can be sent with
`start_request` and the appropriate extension headers; this does not implement a
subscription manager. `start_refer` and `start_notify_refer` build the transfer headers and SIP-fragment
body and use ordinary owned operations. The legacy `reinvite_with_glare_retry`
corresponds to opt-in `start_reinvite_with_retry` (or its Body helper).
`GlareRetryPolicy.max_attempts` includes the first attempt. Only a peer 491 is
retried, after RFC 3261 section 14.1 jitter; every completed attempt and response
is retained. Explicit cancellation or dialog termination stops further attempts.
Dropping the observer or closing admission does not cancel an accepted retry
sequence. Each retry gets its own CSeq and operation identity. The same SDP offer
and headers are reused: the application must cancel when its intent changes.
The basic `start_reinvite` remains one attempt. PRACK and session timers can be configured on
the managed layer, but changing 100rel settings on a live individual dialog is
not exposed. Manual CSeq reservation and mutable live-state access are intentionally
not mirrored.

## Outgoing call

Unlike dropping a legacy INVITE future, dropping the managed observer does not
cancel the call. Retain the operation in a call actor and explicitly call
`initial.cancel()` or `dialog.start_termination(...)` when cancellation is wanted.
Always observe the eventual INVITE result, including a possible late success.

```rust,no_run
use rsipstack::dialog::v2managed::{Body, DialogLayer, InviteOptions, InviteDialog};
# type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
async fn dial(layer: &DialogLayer, options: InviteOptions, offer: Vec<u8>) -> Result<InviteDialog> {
    let (events, _receiver) = layer.new_dialog_event_channel();
    // A real call actor also consumes the event receiver for incoming requests.
    let (dialog, initial) = layer.start_invite(options.body(Body::sdp(offer)), events)?;
    let result = initial.wait().await;
    // Inspect a received response even if later ACK or storage processing failed.
    if let Some(response) = result.response() { println!("{}", response.status_code); }
    result.signaling_result().map_err(Clone::clone)?;
    Ok(dialog)
}
```

## Incoming call and ordered event handling

Route matching in-dialog transactions through `start_incoming_request` in
receive order, before independently scheduling application handlers. The stack
owns transaction processing. Replies, CANCEL and ACK must remain routable while
application work is draining.

```rust,no_run
use rsipstack::{dialog::v2managed::*, sip::{Method, StatusCode}};
# type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
async fn accept(incoming: IncomingInvite, answer: Vec<u8>) -> Result<()> {
    let dialog = incoming.dialog();
    dialog.start_response(StatusCode::Ringing, None, None)?.wait_sent().await?;
    let receipt = dialog.start_response_body(StatusCode::OK, None, Body::sdp(answer))?;
    receipt.wait_sent().await?;
    let matching_ack = receipt.wait_ack().await?;
    println!("ACK: {}", matching_ack.method);
    Ok(())
}
async fn events(mut receiver: DialogEventReceiver) -> Result<()> {
    while let Some(event) = receiver.recv().await {
        match event {
            DialogEvent::Notification(notification) => println!("{notification:?}"),
            DialogEvent::IncomingRequest(request) => {
                let status = if request.request().method == Method::Options {
                    StatusCode::OK
                } else { StatusCode::NotImplemented };
                request.start_response(status, None, None)?.wait_sent().await?;
            }
        }
    }
    Ok(())
}
```

The two event variants share one queue and retain their relative delivery order.
`DialogState` is only a read-only phase and never carries response capabilities.
Protocol notifications can repeat after mid-dialog exchanges. Use the optional
lifecycle fact stream for exactly-once initial-answer facts and occurrence times.

When admission is closed, a rejected `start_incoming_invite` returns
`IncomingInviteError.transaction = Some(transaction)`. The application can send
its chosen rejection with the transaction response API; the layer does not pick
an overload policy. Errors after core admission may return no transaction.

## B2BUA renegotiation

Both legs keep independent identity, CSeq and routing. Initial call establishment,
media policy and decisions to end both legs remain application responsibilities.
The same bridge engine is used under each public interface.

```rust,no_run
use rsipstack::dialog::v2managed::{IncomingRequest, InviteDialog, ReinviteBridge};
# type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
async fn bridge_reinvite(a: InviteDialog, b: InviteDialog, request: IncomingRequest) -> Result<()> {
    let bridge = ReinviteBridge::new(a, b)?;
    let (source, message, responder) = request.into_parts();
    // The call actor dispatches only the matching re-INVITE here.
    bridge.handle(source, message, responder).await?;
    Ok(())
}
```

## Recovery

Preserve stored CSeq, routes and runtime metadata. A default runtime value is not
proof that an old record has no unfinished signaling. Recovery never replays an
interrupted transaction automatically.

```rust,no_run
use rsipstack::dialog::v2managed::{DialogLayer, DialogSnapshot, RestoreOutcome};
# type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn restore(layer: &DialogLayer, snapshot: DialogSnapshot) -> Result<()> {
    let (events, _receiver) = layer.new_dialog_event_channel();
    // Attach the receiver to the recovered call actor in a real application.
    match layer.restore_from_snapshot_detailed(snapshot, events, None)? {
        RestoreOutcome::Restored(id) | RestoreOutcome::AlreadyPresent(id) => {
            let dialog = layer.get_dialog(&id).ok_or("missing restored dialog")?;
            println!("{}", dialog.id());
        }
        RestoreOutcome::RecoveryRequired(snapshot) => {
            println!("application recovery required: {:?}", snapshot.runtime.pending);
        }
        RestoreOutcome::SkippedNonConfirmed(state) => println!("not resumed: {state:?}"),
        _ => {}
    }
    Ok(())
}
```

## Diagnostics and bounded drain

`layer.close_admission()` permanently rejects new normal application commands,
new calls and restoration into that managed layer. It does not stop the endpoint,
terminate quiet calls, stop configured session timers, or disable incoming SIP.
Explicit cancellation, termination, recovery cleanup and responses stay available.
For example, a B2BUA can close admission, request termination of its calls, and
then drain. A durable call service can leave quiet calls alive and persist them.

`layer.drain(budget).await` also closes admission. It observes accepted managed
operations even when callers dropped their handles, includes coherent core
pending-signaling markers (including internal extensions), and awaits persistence
queue cutoffs. The work list and read-only diagnostics share the same tracker.
Later incoming signaling or timer refreshes can extend the wait. Timeout stops
waiting, never cancels operations or reopens admission. A failed storage cutoff
is reported separately from whether all work finished. Do not await drain from a
storage callback or from an application handler whose response it is waiting for.

A successful report means the observed signaling work and persistence cutoffs
finished. It does **not** mean all SIP transactions, retransmission timers, ACK
caches or endpoint tasks retired, nor that a peer cannot send another request.
Continue routing until the application's transport shutdown policy permits it.
Each dialog snapshot is coherent; the aggregate report is a sequential observation,
not an atomic view across dialogs and persistence queues. Snapshot data is owned;
changing it cannot change live state.

```rust,no_run
use rsipstack::dialog::v2managed::DialogLayer;
use std::time::Duration;
async fn inspect_shutdown(layer: &DialogLayer) {
    layer.close_admission();
    // Application-specific call termination, if wanted, goes here.
    let report = layer.drain(Duration::from_secs(5)).await;
    println!("drained={} remaining={:?}", report.is_drained(), report.remaining);
    for cutoff in report.persistence { println!("{cutoff:?}"); }
}
```

`Body::sdp(existing_vec)` moves the existing allocation. Transport serialization
and protocol retention can still require copies; this is not a claim of end-to-end
zero-copy SIP. Raw byte methods remain available for forwarding arbitrary bodies.


## Event publication

`layer.publication(PublicationOptions::new(caller, resource, "presence"))` creates
an owned RFC 3903 publication context. `start_publish(Body, expires)` creates or
modifies state; `start_refresh(expires)` sends an empty body and the latest ETag;
`start_remove()` sends Expires: 0 and remains available during admission closure.
Requests to the same resource within the layer are serialized through their final
responses. Authentication retries retain their response history. PUBLISH does not
learn To tags, Contact targets or Record-Route from a response.

`ExchangeOperation::tracking_id()` identifies the entry in layer diagnostics.
Results preserve observed SIP responses separately from signaling errors.
`Publication::status()` exposes the last known ETag, expiry and uncertainty.
A 412 invalidates that context's ETag; retrying an initial publication with state
is an explicit application decision. A malformed successful response or transport
timeout must not be mistaken for proof that remote state was unchanged.

For a server, `layer.incoming_publication(transaction)` returns an owned request
with `event()`, `if_match()` and `expires()` accessors. The application authorizes
the event package, atomically updates its state store, and then uses
`start_accept(etag, granted_expiry)` or `start_response(status, headers)`.
The receipt observes transmission. Dropping every responder without replying
produces 501; incoming exchanges participate in the layer drain.

Publication contexts have no BYE or re-INVITE methods. Automatic refresh scheduling,
publication-store persistence and recovery of publication ETags remain application
policy. The dialog checkpoint configuration applies to INVITE dialogs and does
not silently persist event publication state.


## Standalone subscriptions

`SubscriptionLayer::new(endpoint)` owns a separate subscription registry.
Alternatively, retain `dialog_layer.subscriptions()` to share command admission
and pending-operation accounting with an existing managed dialog layer. Clones
of a subscription layer share the same registry; repeated calls to
`subscriptions()` create independent registries.

```rust,no_run
use rsipstack::dialog::v2managed::{
    subscription_event_channel, SubscriptionLayer, SubscriptionOptions,
    SubscriptionEventType,
};
use rsipstack::sip::Uri;
async fn subscribe(layer: &SubscriptionLayer, local: Uri, resource: Uri)
    -> Result<(), Box<dyn std::error::Error>>
{
    let (events, mut notifications) = subscription_event_channel();
    let options = SubscriptionOptions::new(
        local.clone(), resource, local, SubscriptionEventType::new("presence")?, 300,
    );
    let (attempt, operation) = layer.start_subscription(options, events)?;
    let completion = operation.wait().await;
    completion.signaling_result().map_err(Clone::clone)?;
    // Also inspect the actual final SIP status; a received rejection is not a send error.
    println!("{:?}", completion.response());
    // An endpoint dispatcher must concurrently pass incoming NOTIFY to this layer.
    // A 2xx SUBSCRIBE alone does not establish a subscription usage.
    if let Some(event) = notifications.recv().await { println!("{event:?}"); }
    for branch in attempt.branches() { println!("{:?}", branch.status()); }
    Ok(())
}
```

The dispatcher calls `receive_notify(transaction)` synchronously in receive
order, then passes `IncomingNotify` to its event-package policy. Valid NOTIFY can
arrive before the initial SUBSCRIBE response; different remote tags create
independent branches. Matching includes the complete dialog identity and Event
package/id. Routing is learned from NOTIFY, not the initial SUBSCRIBE response.
`start_response(200, None)` accepts a notification through a response receipt.
Rejected dispatch returns the transaction and suggested status, allowing the
application to send the error through `Transaction::start_final_response`.

`SubscriptionDialog::start_refresh(expires, body)` and `start_unsubscribe()`
return owned `ExchangeOperation` observers. Refreshes arm Timer N, including
authentication retries. Unsubscribe waits for a terminal NOTIFY; a successful
response alone does not end the usage. New refreshes stop after admission closes;
unsubscribe and required incoming protocol work remain available.

For a notifier, use `receive_subscribe(transaction, NotifierOptions, events)` for
initial requests and refreshes. Authorize and validate the event package first.
`start_accept(granted_expiry, NotifyState, body)` queues both the response and the
mandatory NOTIFY, returning separate observers for them. The granted expiry cannot
exceed the request. Expires=0 produces a terminal notification. `start_reject`
handles authentication, unsupported packages and application rejection.
`SubscriptionDialog::start_notify` sends later resource updates. Policy work that
changes the resource representation should be serialized by the application.

Expiry and missing NOTIFY terminate usages and remove active registry entries;
notifier expiry also sends a terminal NOTIFY. A bounded terminal responder accepts
late terminal NOTIFY crossing local expiry without resurrecting the subscription.
Fork discovery and terminal responder retention last at most the endpoint's
64*T1 interval (restarted for authentication attempts). `diagnostics()` exposes
these counts separately from active usages and accepted operations.

Drain waits for admitted exchanges, incoming response handlers and required
NOTIFY waits. It leaves quiet live subscriptions intact and does not wait for
fork discovery, terminal responder retention or transaction-cache retirement.
Applications choose whether to unsubscribe before draining.

These are standalone usages: sharing an INVITE dialog with subscription usages,
implicit REFER subscriptions, event-list/body composition, authentication of
incoming requests, automatic client refresh policy and subscription persistence
remain separate responsibilities. INVITE snapshot/checkpoint settings do not
persist subscription state. Unsupported extensions such as conditional 204
notifications are not negotiated by this interface.

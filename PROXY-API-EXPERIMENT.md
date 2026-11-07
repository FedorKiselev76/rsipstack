# Stateful proxy API experiment

`rsipstack::proxy` provides owned forwarding independently of `dialog`. It uses
the existing transaction and transport implementation. It does not allocate a
second Call-ID, dialog tags, CSeq or SDP. Applications authorize requests and
select destinations; SIP routing and transaction mechanics belong to the stack.

```rust,no_run
use rsipstack::proxy::{ProxyConfig, ProxyLayer, ProxyTarget};
# async fn example(endpoint: rsipstack::transaction::endpoint::EndpointInnerRef,
# incoming: rsipstack::transaction::Transaction) -> Result<(), Box<dyn std::error::Error>> {
let proxy = ProxyLayer::new(endpoint, ProxyConfig::new(
    "sip:proxy.example.test:5060;transport=udp".parse()?,
)?);
// The application has already authorized this incoming request.
let operation = proxy.forward(incoming, ProxyTarget::next_hop(
    "sip:gateway.example.test:5060;transport=udp".parse()?,
));
let first_final = operation.wait_final_response().await?;
// More successful fork/retransmission responses may follow the first final.
let retired = operation.wait().await?;
# Ok(()) }
```

`ProxyTarget::next_hop` selects a destination without changing Request-URI.
`with_request_uri` applies an explicit location-service rewrite.
`ProxyTarget::routed()` follows Route or Request-URI. Remaining Route headers
take precedence over a supplied default next hop. Only the leading local Route
is consumed; strict routers retain the remote target in the final Route value.
The default config inserts the local loose Record-Route on an initial INVITE.

The layer decrements Max-Forwards, adds its Via, and removes exactly that Via from
responses (including comma-folded headers). Call-ID, From/To, CSeq, Contact,
message body and end-to-end extensions remain unchanged. Unsupported
Proxy-Require receives 420 with Unsupported. Exhausted Max-Forwards receives 483.
SIPS destinations cannot silently downgrade to an insecure next hop or advertised
Record-Route; this is not a claim of complete TLS interoperability coverage.

Each accepted forwarding operation owns its protocol work. Dropping observers
does not stop it. `wait_final_response` observes the first final response sent
upstream; `wait` also waits for both transaction owners and generated CANCEL to
retire. Results retain the observed response when a later stage fails. Progress
stores only the first final and latest response, not an unbounded packet history.
`progress().request` is preparation evidence, not proof of network delivery.

CANCEL is hop-by-hop and waits for a downstream provisional response. It uses one
Via and the original branch/CSeq, and carries Reason from the incoming CANCEL.
A successful CANCEL does not replace the INVITE outcome. Timer C refreshes on
101..199, requests cancellation on expiry, and bounds the wait for a final reply.
A locally generated timeout does not produce an ACK to an unobserved peer reply.
Non-2xx ACK is handled by transactions.
Successful INVITE responses and their retransmissions/forks reach the caller;
their ACKs remain end-to-end. The proxy does not independently retransmit 2xx or
generate PRACK. Reliable provisional retransmissions pass upstream unchanged.

Creating a ProxyLayer enables otherwise unmatched ACK delivery on the endpoint's
incoming channel. The application's router must authorize and forward these too.
Such ACKs do not acquire server transaction state: a second ACK is still delivered
while the first awaits policy or transport. Existing UA dialogs match their own
ACKs first. Endpoint shutdown forcibly interrupts pending operations with
`DriverStopped`; it is not a graceful-drain guarantee.

## Scope and validation

This is one selected next hop per request, not a location database, topology
hiding, parallel/sequential forking or a complete OpenSIPS replacement. All 2xx
from a downstream forking proxy are relayed. Stateful call billing, media policy,
dialog recovery and whole-call termination belong to other application layers.
Authorization, admission limits and routing policy remain application-owned.

Public UDP tests cover identity/body preservation, folded Via, forked and repeated
2xx, repeated ACK while a prior ACK is owned, CANCEL before provisional, non-2xx
ACK, loose/strict routes, challenge passthrough, REGISTER/OPTIONS/INFO/UPDATE/PRACK/
re-INVITE/BYE, reliable provisional retransmissions, validation, SIPS downgrade
rejection, Timer C refresh/cancellation/timeout and shutdown cleanup.
`examples/interop_proxy.rs` exercises this API
with the existing eleven external-peer UDP/TCP scenarios:

```sh
cargo test --offline --test proxy_operations
bash scripts/interop/run.sh proxy
```

Protocol references: [RFC 3261 section 16](https://www.rfc-editor.org/rfc/rfc3261.html#section-16)
and [RFC 6026](https://www.rfc-editor.org/rfc/rfc6026.html).

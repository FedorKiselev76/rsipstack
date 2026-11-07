# Experimental dialog API design

This document describes the design behind the prototype. The
[managed API guide](MANAGED-DIALOG-API.md) documents the implemented public
interface with examples. The package version in this checkout is experimental.

## Objective and ownership

An application chooses call behavior; rsipstack executes SIP correctly. The
application can inspect messages, choose status codes, supply SDP and extension
headers, select destinations, and implement media and business policy. It should
not implement CSeq allocation, dialog matching, ACK caches, retransmissions or
CANCEL/BYE races to use the managed dialog API.

Existing consumers may be rewritten. Their required behavior is the acceptance
criterion; their actors, database layouts, first-poll tricks and prepared SIP
requests are not API requirements.

| Responsibility | Owner |
| --- | --- |
| Identity, CSeq, route set, target refresh, transaction matching, retransmissions, ACK machinery | Stack |
| Admission and protocol/offer-answer exchange ordering | Stack |
| Destination, response status, SDP content, codecs, media and coupling between legs | Application |
| Decide when to cancel or hang up | Application; stack executes the signaling |
| Occurrence-time lifecycle facts and individual operation results | Stack |
| Billing, CDR, provider selection, business idempotency and external effects | Application |
| Coherent checkpoints and waiting for acknowledgement | Stack |
| Durable commit, schema migration, outbox and recovery policy | Application |

Storage, lifecycle subscribers, PRACK, session timers and bridging remain optional.
No media engine or database becomes a dependency. Low-level transaction and
transport APIs remain available for proxies and specialized protocol work.

The layering is informed by PJSIP's [Base Dialog](https://docs.pjsip.org/en/latest/api/generated/pjsip/group/group__PJSIP__DIALOG.html)
and [INVITE Session](https://docs.pjsip.org/en/latest/api/generated/pjsip/group/group__PJSIP__INV.html):
dialog mechanics and INVITE-session behavior are distinct from media handling.
This is an architectural reference, not copied code or a claim that PJSIP provides
our persistence and recovery contracts.

## 1. Commands and owned operations

Keep `InviteDialog` as the established-session handle. The experimental
`start_request` and `start_reinvite` methods return a `DialogOperation`
synchronously after admission. The caller does not supply CSeq, tags, Via or
Route to these operations.

This is an excerpt from the runnable [example](examples/dialog_operations.rs);
the surrounding function supplies the dialog, SDP and imports:

```rust,ignore
let operation = dialog.start_reinvite(
    Some(vec![Header::ContentType("application/sdp".into())]),
    Some(offer),
    ReinviteOptions::default(),
)?;
let result = operation.wait().await;
let response = result.response();
let signaling = result.signaling_result();
let state_at_completion = result.dialog_state();
```

Operation identity is available at admission and is independent of SIP CSeq.
Authentication and 422 retries can create several attempts within one operation;
retransmitting a packet does not create another attempt. Each attempt retains its
request and observed final response. Operation sequences are scoped to a live
dialog instance, not global database idempotency keys.

The target API must also carry optional application correlation context, supplied
at admission, into attempts and checkpoints. A checkpoint can arrive before the
caller registers the returned operation ID. Predicting CSeq or matching only by
method cannot solve that race. This context is not implemented in the prototype.

All managed request producers must share CSeq/first-send ordering: commands,
authentication/interval retries, automatic refresh and cleanup, and retained
convenience methods on managed handles. Responses can complete out of order; the
queue must not serialize entire transactions. ACK/CANCEL retain their own protocol
sequence rules and cannot wait as ordinary new requests. CSeq rules are specified
by [RFC 3261 section 12.2.1.1](https://www.rfc-editor.org/rfc/rfc3261.html#section-12.2.1.1).

Overlapping SDP negotiation has an explicit rejection or configured retry policy.
Extension headers cannot override stack-owned identity/routing, including through
compact aliases or untyped headers. Expert raw mutation belongs to the low-level
API with its own responsibilities.

## 2. Separate response, signaling and session state

| Boundary | Meaning |
| --- | --- |
| Admitted | The stack owns the accepted command; a send is not promised yet. |
| Checkpoint committed | The exact checkpoint was acknowledged; distinct from storage being disabled. |
| Transport accepted | A send succeeded; remote receipt is not established. |
| Response observed | Matching SIP response, with peer/local/unknown provenance and attempt identity. |
| Signaling completed | Required method-specific signaling, including ACK work, completed or failed. |
| Session state at completion | State captured with the result; later state can be read from the dialog. |

A peer 488 can be a successfully completed exchange that rejects an SDP change.
A peer 408 differs from a locally generated timeout. A received 200 remains
accessible if storage or ACK subsequently fails. Earlier responses remain in
attempt history if a retry fails before receiving another response.

A late re-INVITE 200 cannot reactivate an ended session. ACK/retransmission duties
continue for their protocol lifetime; see [RFC 6026 section 7.2](https://www.rfc-editor.org/rfc/rfc6026.html#section-7.2).
An additional cleanup BYE after an already completed BYE is not made a universal
policy. If needed by an application, it requires a separately validated cleanup
operation, not mutating the dialog back to Confirmed or hand-building SIP state.

Dropping a handle or wait future drops an observer. Explicit INVITE cancellation
still observes final-response/ACK races. A queued cancellation before construction
can complete without allocating CSeq. Non-INVITE methods do not acquire fictitious
SIP CANCEL semantics. A local wait deadline does not cancel owned work.

This distinction must be prominent in the public API description and migration
examples. Applications that previously cancelled a dialing attempt by dropping
an INVITE future must explicitly call `start_cancel()` or, to abandon the whole
attempt across final-response races, `start_termination()`. Retain handles for
parallel attempts and explicitly terminate losing legs. A CANCEL 200 and the
initial INVITE outcome remain separate observations. See the compiled
[dialing-deadline example](MIGRATING-0.9.md#waiting-deadlines-and-explicit-cancellation).
Retaining legacy APIs does not change their existing future-drop contract.

Target result types use private construction and extensible enums. Disabled,
not-attempted, failed and completed stages must be distinguishable. The current
boolean attempt flags are a limited prototype, not the final stage model.

## 3. Initial INVITE and incoming response control

Preserve current creation and `accept`/`reject` methods. Complete the managed API
with two additive capabilities on the existing transaction machinery:

- An owned initial-INVITE operation: stable identity before a remote tag exists,
  cancellation, provisional/final observations, selected-dialog result even if
  already ended, and ownership of late/forked successful-response cleanup.
- An incoming response receipt: admission, persistence and transport outcome,
  with a separate matching-ACK wait for INVITE. Queuing 200, sending it and
  receiving ACK are different facts. Duplicate final-response submissions are
  explicitly rejected/reported instead of causing a second final send.

Incoming controls identify the original request. The stack resolves deadline,
CANCEL and final-response races. The application supplies policy and, where
necessary, SDP through callbacks outside internal locks. SDP computation has a
deadline and a defined fallback. Existing `InviteAnswerHandler` provides the
starting point for offerless exchanges. Use the matching request's ACK, not a
dialog-wide last ACK.

The experiment now provides `start_termination(TerminationOptions)` for a common
termination intent across early, WaitAck and confirmed sessions. The application
selects the unanswered incoming-call status and optional Reason; the stack
selects CANCEL, rejection or BYE. The first admitted options win and repeated
calls observe the same operation. For an outgoing early call, the owning initial
operation handles late success with ACK followed by BYE. An incoming success
waits for matching ACK or the ACK timeout before local BYE.

`TerminationCompletion` retains separate cancellation, initial INVITE, rejection
and BYE results when applicable. Session termination and completion of that
signaling are separate boundaries. Existing raw initial-call APIs remain available;
early outgoing calls need `start_invite` to use managed termination. Termination
completion does not imply retirement of every transaction or a global drain.

Advanced early branching belongs to the initial-INVITE controller. Each branch
has immutable identity and its own SDP history. The simple path selects one;
advanced policy observes/selects branches while the stack owns ACK and cleanup.
Sequential provider failover must not reuse one branch's SDP identity for another.
This is a separate extension, required before claiming full migration of a
branching signaling application. It is not implemented by the confirmed-dialog
operation prototype, and must not be emulated by a public tag setter.

## 4. Persistence through checkpoints

The shared ordering is:

```text
command + optional application context
  -> admission / operation handle
  -> stack allocates CSeq and constructs the final request
  -> coherent checkpoint + request + correlation context
  -> application commits snapshot and related business state/outbox
  -> acknowledgement
  -> transport send
  -> response / ACK / completion checkpoints
```

Configure `DialogPersistence` before creation/restoration. A writer can commit
stack and business data atomically in its own database. Writers are serialized
per dialog, not globally. Callbacks run without dialog locks and must not await
an actor/operation that depends on their own acknowledgement.

There is no new prepare/start protocol or application CSeq reservation. Legacy
`prepare_reinvite` retains its limited compatibility contract; it is not the
recommended persistence mechanism.

Storage failure/timeouts stop subsequent protected sends. Timeout or worker loss
can leave commit outcome unknown. Adapters must classify ambiguous database
outcomes conservatively; a generic storage error must not imply nothing committed.
There is no atomic commit-and-network-send guarantee or automatic SIP replay.

`persistence_barrier()` synchronously captures a FIFO cutoff of accepted writes,
including different writes at the same revision. Later writes do not extend its
`wait()`. Dropping the waiter does not cancel writes. Disabled storage completes
immediately. A barrier is not a signaling/transport drain.

Lifecycle notifications are optional observations with occurrence time and
sequence. In durable mode, release follows the covering commit while retaining
occurrence time. Duplicate 200 responses do not create another initial-answer
fact. Subscribers do not block SIP. Billing and exactly-once external effects
remain application policy; an outbox may be part of the checkpoint transaction.

## 5. Recovery without invented quiescence

`DialogSnapshot` is data, not mutable live state. Its builder reconstructs stored
values explicitly; restore validates again. Preserve routing, counters, lifecycle
and pending metadata. Missing legacy metadata requires a migration decision,
not an automatic assumption of no pending work. See [MIGRATING-0.9.md](MIGRATING-0.9.md).

`restore_from_snapshot_detailed` distinguishes restored, already present, skipped
non-confirmed state and `RecoveryRequired` with the unchanged snapshot. Invalid
records fail validation. Legacy boolean wrappers retain their meanings. Restore
never replaces an existing dialog or replays lifecycle notifications.

Interrupted work needs a restricted recovery controller: normal INFO/re-INVITE
remains unavailable and original pending evidence is retained. Application policy
may request termination only when identity, CSeq/routing and signaling obligations
can be established from saved evidence. Missing ACK/SDP or transport information
produces an explicit insufficient-evidence/unsupported outcome. Never clear pending
flags, label incomplete state quiescent, replay old requests speculatively, or
promise remote cleanup. Generic interrupted-transaction replay is outside scope.

The current experiment implements a narrow `start_recovery_termination` operation:
it sends a fresh BYE, retains interrupted evidence and never exposes a live dialog.
It rejects an unfinished initial ACK. It cannot reconstruct lost ACK bodies,
resume media negotiation, guarantee a peer still uses the saved target or replay
interrupted transactions. Recovery policy must account for those limits.

Missing optional runtime capabilities must be reported too: reconstructing a
confirmed dialog does not prove that session-timer scheduling resumed.

## 6. Shutdown and ownership

Session termination is distinct from transaction retirement, storage completion,
registry removal and task release. The target managed controller must:

1. Close command admission at a defined cutoff while retaining required protocol
   handling for accepted work and incoming signaling.
2. Let the application choose termination or capture for recovery, observing the
   selected operations and incoming-response work.
3. Await storage covering that signaling work; an earlier barrier is insufficient.
4. Await protocol retirement and release registry/task ownership before stopping
   transports/executor. A deadline reports pending/unknown work, not success.

Ordinary handle drop is not hangup. Applications should not poll private maps to
infer shutdown. Queue limits and explicit overload results are release gates.
Slow diagnostics must not block SIP; lost diagnostic observations are reported,
while command results and durable facts retain their owning records.

## 7. Compatibility: one protocol implementation

Add operations, barriers and detailed restore alongside current methods. Do not
silently change legacy future-drop behavior or the meaning of successful
`accept()`/`respond()` calls, which currently acknowledge queue admission.

The prototype retains public raw `DialogInner` fields/constructors. New operations
require layer-owned state or validated restoration that copies data; externally
retained raw state is rejected with `ExternallyOwnedState`. Direct field writes
bypass snapshot synchronization. Tests cover that distinction; constructor,
setter, registry and convenience-method paths still need a complete audit.

Retained methods on managed state must use the same internal ordering and
ownership mechanisms. There must not be two independent SIP engines. If a bypass
cannot be isolated compatibly, propose that specific breaking change with a
migration example. The separate snapshot builder migration does not justify
making every old field private.

## 8. Capability check across three application styles

| Application style | Retains | Removes through the managed API |
| --- | --- | --- |
| Durable outbound call service | Origination, media, billing, CDR/outbox, restart policy | Manual requests, guessed receipt times, first-poll synchronization, invented safe recovery |
| Programmable signaling agent | Routing scripts, provider failover, business jobs, independent leg policy | Application CSeq allocation, prepared-message maps, fork ACK caches, tag mutation |
| Media B2BUA | Media resources, per-leg codec/SDP decisions, keepalive and whole-call teardown policy | Transaction lifetime bookkeeping, manual ACKs, treating queued responses as sent or probes as call state |

All three use the same command/observation/checkpoint contract. A B2BUA owns
independent dialogs: use `ReinviteBridge` for forwarding negotiations when
appropriate, or negotiate each leg independently when terminating media. Neither
mode shares Call-IDs, tags or CSeq across legs.

Full acceptance requires both original roles, early branches, offerless exchanges,
cancellation races, storage/restart behavior and bounded cleanup through public
APIs. Confirmed-dialog tests alone do not prove feature parity in all consumers.

## Implementation sequence

1. Complete common ordering, correlation, attempt outcomes and compatibility tests;
   preserve observed responses through every failure path.
2. Add owned initial-INVITE and incoming-response receipts; validate both roles,
   offerless exchanges and early branches using the existing protocol machinery.
3. Add restricted recovery and managed shutdown with explicit failure outcomes.
4. Rewrite consumer adapters against an exact tested revision. Test ordinary calls
   without optional features, durability/recovery and independent B2BUA legs.
5. Run external-peer regressions and separate soak tests, then prepare upstream
   proposals/PRs and migration examples for the final public surface.

This design does not certify a completed migration or production readiness. The
accompanying status document records what the current experiment actually proves.

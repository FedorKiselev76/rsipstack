# Snapshot migration for the proposed 0.9.0 release

These changes require a breaking release. The proposed target is **0.9.0**,
subject to maintainer approval; this document does not announce a release or
change the package version. Do not backport this API change into 0.8.x.

## Constructing and reading snapshots

`DialogSnapshot` now includes coherent runtime metadata and is
`#[non_exhaustive]`. Adding `runtime` breaks old struct literals and exhaustive
patterns. The attribute also prevents external struct literals, even if they
include `runtime`. Replace them with `DialogSnapshot::builder(...)`; adding
`runtime: DialogSnapshotRuntime::default()` to a literal is not sufficient.

Supply the stored identity, role, From/To headers, remote URI, state, both CSeq
counters, and runtime metadata. The builder requires state, counters and runtime
explicitly: it never assumes a stored dialog was confirmed or quiescent. Copy
optional stored contacts, routes, 100rel support and Session-ID through their
builder methods too. Omitting those optional settings yields `None`, no routes,
`false`, and `None`, respectively; it does not recover their persisted values.

This example illustrates an application-approved migration of a legacy,
confirmed record known to have been captured with no signaling in flight:

```rust
use rsipstack::{
    dialog::{
        dialog::{DialogSnapshot, DialogSnapshotState},
        snapshot::DialogSnapshotRuntime,
        DialogId,
    },
    sip::{headers, prelude::ToTypedHeader},
    transaction::key::TransactionRole,
};

let snapshot = DialogSnapshot::builder(
    DialogId {
        call_id: "stored-call".into(),
        local_tag: "local".into(),
        remote_tag: "remote".into(),
    },
    TransactionRole::Client,
    headers::From::from("<sip:alice@example.com>;tag=local").typed()?,
    headers::To::from("<sip:bob@example.com>;tag=remote").typed()?,
    "sip:bob@example.com".try_into()?,
)
.state(DialogSnapshotState::Confirmed)
.local_cseq(42)
.remote_cseq(7)
.local_contact(Some("sip:alice@example.com".try_into()?))
// The application has established that this legacy record was quiescent.
.runtime(DialogSnapshotRuntime::default())
.build()?;

// Field access is unchanged. Patterns must allow future fields.
let DialogSnapshot { id, runtime, .. } = snapshot;
assert_eq!(id.call_id, "stored-call");
assert!(runtime.lifecycle.is_empty());
# Ok::<(), Box<dyn std::error::Error>>(())
```

The builder checks required inputs and the lifecycle history of confirmed
snapshots. It also represents pending and non-confirmed snapshots so applications
can inspect or store them. `build()` succeeding does **not** guarantee that
restore will resume the dialog. Public fields remain mutable, so restore retains
its own validation.

## Storage and recovery behavior

The application owns serialization, storage schema versions and migrations.
`runtime.revision` identifies a dialog state revision, **not a storage schema**.
Persist the complete runtime metadata, including pending operations and lifecycle
records. Reconstruct individual facts with
`DialogLifecycleRecord::new(sequence, since_unix_epoch, kind)`. Preserve their
sequence and optional wall-clock time; do not invent timestamps or acceptance
events during migration. Restoring retains the history without replaying it to
notification consumers. Monotonic timestamps are process-local and not stored.

For a pre-0.9 record without runtime metadata, explicitly passing
`DialogSnapshotRuntime::default()` represents revision zero, no recorded history,
and no pending operations. This is appropriate only when the application's
existing capture contract or other reliable evidence establishes that signaling
was quiescent. Missing metadata alone does not establish that fact. Empty history
means no history was retained; it is not evidence that the call was never answered.
If quiescence cannot be established, apply an explicit application recovery policy
instead of silently resuming the call. Never discard known pending work or use a
default to hide corrupt metadata.

Calls to `dialog.snapshot()` and `layer.restore_from_snapshot(snapshot, sender)`
keep their signatures. The restore behavior changes as follows:

- A confirmed snapshot with non-empty `runtime.pending` returns an error. SIP
  transactions, outstanding ACKs and application handlers are not replayed.
- Invalid confirmed lifecycle history or revision returns an error.
- Non-confirmed snapshots are still skipped with `Ok(false)`.
- An already registered dialog is still retained and restore returns `Ok(false)`.

Future optional fields may have safe defaults without breaking builder callers.
New data required for safe recovery must instead have an explicit migration or
compatibility plan. `#[non_exhaustive]` protects source construction and matching;
it is not a promise that every future persisted record can be silently migrated.

## Owned dialog operations and legacy methods

The experimental dialog API keeps the existing signaling methods public and
marks them `#[deprecated]`. An application using only those methods can continue
using the legacy contract; deprecation itself does not change execution. This
is not a promise of source compatibility for other 0.9 changes, including the
snapshot builder migration above.

| Legacy call | Managed replacement |
| --- | --- |
| `do_invite`, `do_invite_async` | `DialogLayer::start_invite`, then `wait()` |
| Manual initial UAS driver | `DialogLayer::start_incoming_invite` |
| `InviteDialog::handle`, `Dialog::handle` | Synchronous `start_incoming_request(tx)`, then observe `wait()` |
| `request`, `info`, `options`, `update`, `message`, `refer`, `notify` | `start_request` with the method, application headers and body |
| `prepare_reinvite`, `send_reinvite`, `reinvite` | `start_reinvite` with offer/answer policy |
| `bye`, `hangup` and their variants | `start_termination` with `TerminationOptions` |
| Initial `cancel` | `start_cancel` for explicit CANCEL observation, or `start_termination` to end the session |
| `ringing`, `accept`, `reject`, `TransactionHandle::reply/respond` | `start_response`, then `wait_sent()` |
| Local response without a dialog | `Transaction::start_final_response`, then `wait_sent()` |

Migrated adapters can use `#![deny(deprecated)]` to make accidental legacy calls
compile errors. Low-level transaction APIs remain available for intentional
transaction consumers. Mixing legacy signaling and managed operations on the
same dialog is not covered by the new ownership guarantees: legacy methods can
bypass admission, send ordering and operation receipts.

Call `start_incoming_request` directly from the receive loop, before spawning a
completion observer. This validates remote CSeq in receive order; independent
handler tasks and responses may complete in a different order. A preceding ACK
already delivered by the endpoint is processed before the next in-dialog request.

Initial incoming INVITEs transfer their driver to `start_incoming_invite`.
`IncomingInvite::dialog()` exposes the first UAS; `branch()` creates an independent
early identity while sharing one final-response admission. Branching with 100rel
or session timers is rejected until that extension coordination is implemented.
Dropping these observers does not stop accepted signaling.

For a valid early BYE, the owned incoming driver also completes an unanswered
initial INVITE with 487 and retains its retransmission/ACK processing. It does
not replace a final response already admitted by the application. With logical
early branches, BYE for a previous branch does not reject the active response
branch; branch selection follows admitted non-100 responses, not branch creation.
Applications should not manually send another 487 after handing over the INVITE.

### Waiting deadlines and explicit cancellation

This is a semantic migration, not a method rename. Code that abandons a legacy
INVITE future on timeout or when another fork wins must explicitly cancel or
terminate the managed attempt. `timeout(..., operation.wait())`, dropping the
handle, and aborting a task that only observes `wait()` stop observation; they do
not cancel an accepted operation. The stack continues its protocol work while
its runtime and endpoint remain alive.

For a dialing deadline or a losing call leg, request all-phase termination. It
also handles the race where a successful INVITE response arrives as the deadline
expires, without requiring an application state check to choose CANCEL or BYE:

```rust,no_run
# async fn dialing_deadline(
#     layer: std::sync::Arc<rsipstack::dialog::dialog_layer::DialogLayer>,
#     options: rsipstack::dialog::invitation::InviteOption,
#     states: rsipstack::dialog::dialog::DialogStateSender,
# ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let (dialog, invitation) = layer.start_invite(options, states)?;
match tokio::time::timeout(std::time::Duration::from_secs(20), invitation.wait()).await {
    Ok(result) => {
        // Inspect the actual SIP response independently of signaling errors.
        let response = result.response();
        let signaling = result.signaling_result();
    }
    Err(_) => {
        let ending = dialog.start_termination(Default::default())?;
        let completion = ending.wait().await;
        completion.signaling_result().map_err(Clone::clone)?;
    }
}
# Ok(()) }
```

If only the CANCEL exchange needs separate observation, use `start_cancel()`
and retain the initial INVITE operation too. A 200 response to CANCEL does not
establish the INVITE result or prove the session has ended. For parallel dialing,
retain every attempt's dialog/operation and explicitly terminate each loser;
discarding its waiter is insufficient. A separate deadline may bound waiting for
termination, but the accepted cleanup continues after that deadline. This is not
a guarantee of completion after forced runtime shutdown.

Legacy methods keep their existing cancellation contract. They are deprecated
with explicit replacements; the new ownership semantics apply when a consumer
opts into managed operations.

With persistence enabled, a validated remote BYE is included in a
`RemoteTerminationReceived` checkpoint before the terminal checkpoint and its
notifications. Both writes complete before the BYE response is sent. Application
billing and Reason-header policy remain in the persistence adapter.


## Choosing a dialog API without changing existing callers

This experimental separation does not require renaming existing imports.
`rsipstack::dialog::invite_dialog::InviteDialog`, `dialog_layer::DialogLayer`,
and the deprecated Client/Server wrappers keep their existing public paths.
Ordinary builds include both generations, including builds made with
`--no-default-features`. The snapshot schema migration described above is a
separate, previously proposed 0.9 change; the namespace separation does not
undo or add to that storage migration.

New INVITE applications can use:

```rust,no_run
use rsipstack::dialog::v2managed::{
    invite_dialog::InviteDialog,
    termination::TerminationOptions,
};

async fn end_call(dialog: &InviteDialog) -> Result<(), Box<dyn std::error::Error>> {
    let operation = dialog.start_termination(TerminationOptions::default())?;
    let completion = operation.wait().await;
    completion.signaling_result().map_err(Clone::clone)?;
    Ok(())
}
```

Use `v2managed::dialog_layer::DialogLayer` to create or restore managed dialogs.
Create an ordered event channel with `layer.new_dialog_event_channel()`.
Pass the sender when starting or restoring the call. Read
`DialogEvent::Notification` and `DialogEvent::IncomingRequest` from the same
receiver. Incoming requests carry the handle used to respond.

The managed dialog, layer, response handle, events and operation completions
hide compatibility handles and mutable live state. They have no `Deref`,
`AsRef` or public conversion into the old interface. `Request`, `Response`,
configuration and snapshot values remain ordinary owned data. A received
request is answered through `TransactionHandle::start_response`; its receipt
separately observes transmission and the matching ACK.

Migrate call creation and all handling for those calls together. Existing calls
can continue through the old layer while newly selected calls use a managed
layer. Do not assign one live dialog to two owners. Snapshot restoration is a
recovery boundary, not a way to convert an active handle between interfaces.
Only an eligible, quiescent snapshot can resume a confirmed dialog.

Dropping a managed operation or its waiter does not cancel SIP work. Translate
application deadlines and losing fork policy into explicit `start_termination`
or initial-operation cancellation. Existing borrowed-future cancellation
behavior remains in the old adapter.

The managed facade covers INVITE dialogs and their extensions, including
re-INVITE bridging, PRACK/100rel and session timers. Standalone event
subscriptions use `SubscriptionLayer` and `SubscriptionDialog`. Event publication
uses `Publication`. Registration has its own API.

For maintainers, `bash scripts/check-dialog-api-boundary.sh` builds and runs
managed public tests with the internal `rsipstack_no_legacy` cfg. This is an
architectural audit, not a Cargo feature or a supported deployment option.
Consumers do not need to set it. CI runs the audit independently of ordinary
legacy/managed compatibility tests.

Managed examples, capability differences and the operation-drain contract are in
[MANAGED-DIALOG-API.md](MANAGED-DIALOG-API.md).

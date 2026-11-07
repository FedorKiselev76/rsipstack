//! Standalone event subscriptions. Dialog identity/routing and transactions use
//! the common SIP core; event usage state is independent of INVITE call state.
use super::exchange::{self, DriverGuard, ExchangeOperation, Queue, Trace};
use super::*;
use crate::{
    dialog::dialog::DialogInner,
    platform::{self, mpsc, sync::Mutex, CancellationToken, Instant},
    sip::{HeadersExt, Uri},
    transaction::{
        endpoint::EndpointInnerRef, make_tag, transaction::transaction_event_sender_noop,
    },
};
use alloc::{collections::BTreeMap, sync::Weak};
use core::time::Duration;

type Core = Arc<DialogInner>;

/// Event package and optional RFC 3265 compatibility id. Both compare exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionEventType {
    package: String,
    id: Option<String>,
}
impl SubscriptionEventType {
    pub fn new(package: impl Into<String>) -> crate::Result<Self> {
        Self::parse(&package.into())
    }
    pub fn with_id(mut self, id: impl Into<String>) -> crate::Result<Self> {
        let id = id.into();
        if !exchange::token(&id) {
            return Err(invalid("invalid Event id"));
        }
        self.id = Some(id);
        Ok(self)
    }
    pub fn package(&self) -> &str {
        &self.package
    }
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn parse(value: &str) -> crate::Result<Self> {
        let mut parts = value.split(';');
        let package = parts.next().unwrap_or_default().trim().to_string();
        if !exchange::token(&package) {
            return Err(invalid("invalid Event package"));
        }
        let mut id = None;
        for part in parts {
            if let Some((name, value)) = part.trim().split_once('=') {
                if name.trim().eq_ignore_ascii_case("id") {
                    if id.is_some() || !exchange::token(value.trim()) {
                        return Err(invalid("invalid Event id"));
                    }
                    id = Some(value.trim().to_string());
                }
            }
        }
        Ok(Self { package, id })
    }
    fn wire(&self) -> String {
        match &self.id {
            Some(id) => format!("{};id={id}", self.package),
            None => self.package.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SubscriptionOptions {
    pub subscriber: Uri,
    pub resource: Uri,
    pub contact: Uri,
    pub event: SubscriptionEventType,
    pub expires: u32,
    pub accept: Option<String>,
    pub body: Option<Body>,
    pub credential: Option<Credential>,
    pub route_set: Vec<crate::sip::Route>,
    pub destination: Option<crate::transport::SipAddr>,
}
impl SubscriptionOptions {
    pub fn new(
        subscriber: Uri,
        resource: Uri,
        contact: Uri,
        event: SubscriptionEventType,
        expires: u32,
    ) -> Self {
        Self {
            subscriber,
            resource,
            contact,
            event,
            expires,
            accept: None,
            body: None,
            credential: None,
            route_set: Vec::new(),
            destination: None,
        }
    }
}
#[derive(Clone)]
pub struct NotifierOptions {
    pub contact: Uri,
    /// Used only when the incoming SUBSCRIBE omits Expires.
    pub default_expires: u32,
    pub credential: Option<Credential>,
}
impl NotifierOptions {
    pub fn new(contact: Uri, default_expires: u32) -> Self {
        Self {
            contact,
            default_expires,
            credential: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionPhase {
    WaitingNotify,
    Pending,
    Active,
    Terminated(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionRole {
    Subscriber,
    Notifier,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubscriptionStatus {
    pub id: DialogId,
    pub event: SubscriptionEventType,
    pub role: SubscriptionRole,
    pub phase: SubscriptionPhase,
    pub expires_at: Option<Instant>,
    pub notify_deadline: Option<Instant>,
    pub local_cseq: u32,
    pub remote_cseq: Option<u32>,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SubscriptionEvent {
    Changed(SubscriptionStatus),
    /// No NOTIFY established a branch before Timer N expired.
    EstablishmentTimedOut {
        call_id: String,
        local_tag: String,
    },
}
pub type SubscriptionEventSender = mpsc::UnboundedSender<SubscriptionEvent>;
pub type SubscriptionEventReceiver = mpsc::UnboundedReceiver<SubscriptionEvent>;
pub fn subscription_event_channel() -> (SubscriptionEventSender, SubscriptionEventReceiver) {
    mpsc::unbounded_channel()
}

/// The notifier chooses resource state. The stack owns the wire header and expiry.
#[derive(Clone, Debug)]
pub enum NotifyState {
    Pending,
    Active,
    Terminated { reason: String },
}
impl NotifyState {
    pub fn terminated(reason: impl Into<String>) -> Self {
        Self::Terminated {
            reason: reason.into(),
        }
    }
    fn validate(&self) -> crate::Result<()> {
        if let Self::Terminated { reason } = self {
            if !exchange::token(reason) {
                return Err(invalid("invalid termination reason"));
            }
        }
        Ok(())
    }
    fn phase(&self) -> SubscriptionPhase {
        match self {
            Self::Pending => SubscriptionPhase::Pending,
            Self::Active => SubscriptionPhase::Active,
            Self::Terminated { reason } => SubscriptionPhase::Terminated(reason.clone()),
        }
    }
    fn wire(&self, expires: u32) -> String {
        match self {
            Self::Pending => format!("pending;expires={expires}"),
            Self::Active => format!("active;expires={expires}"),
            Self::Terminated { reason } => format!("terminated;reason={reason}"),
        }
    }
}
struct ParsedNotify {
    phase: SubscriptionPhase,
    expires: Option<u32>,
}
fn parse_notify(request: &Request) -> crate::Result<ParsedNotify> {
    let value = exchange::header(request, "Subscription-State")
        .ok_or_else(|| invalid("missing Subscription-State"))?;
    let mut parts = value.split(';');
    let state = parts.next().unwrap_or_default().trim();
    let mut expires = None;
    let mut reason = "terminated".to_string();
    for part in parts {
        if let Some((key, value)) = part.trim().split_once('=') {
            if key.eq_ignore_ascii_case("expires") {
                expires = Some(
                    value
                        .parse()
                        .map_err(|_| invalid("invalid subscription expiry"))?,
                );
            }
            if key.eq_ignore_ascii_case("reason") {
                reason = value.to_string();
            }
        }
    }
    let phase = if state.eq_ignore_ascii_case("active") {
        SubscriptionPhase::Active
    } else if state.eq_ignore_ascii_case("pending") {
        SubscriptionPhase::Pending
    } else if state.eq_ignore_ascii_case("terminated") {
        SubscriptionPhase::Terminated(reason)
    } else {
        SubscriptionPhase::Pending
    }; // RFC 6665: unknown states are pending.
    if !matches!(phase, SubscriptionPhase::Terminated(_)) && expires.is_none() {
        return Err(invalid("active or pending NOTIFY requires expires"));
    }
    Ok(ParsedNotify { phase, expires })
}
fn invalid(message: &str) -> crate::Error {
    crate::Error::Error(message.into())
}
fn event(request: &Request) -> crate::Result<SubscriptionEventType> {
    SubscriptionEventType::parse(
        &exchange::header(request, "Event").ok_or_else(|| invalid("missing Event"))?,
    )
}
fn contact(request: &impl HeadersExt) -> crate::Result<Uri> {
    let contacts = request.typed_contact_headers()?;
    if contacts.len() != 1 {
        return Err(invalid("expected one Contact"));
    }
    Ok(contacts[0].uri.clone())
}
fn next_cseq(sequence: &Mutex<u32>) -> crate::Result<u32> {
    let mut seq = sequence.lock();
    *seq = seq
        .checked_add(1)
        .filter(|n| *n < 1 << 31)
        .ok_or_else(|| invalid("subscription CSeq exhausted"))?;
    Ok(*seq)
}
fn core_for(
    tx: &Transaction,
    id: DialogId,
    contact: Uri,
    credential: Option<Credential>,
) -> crate::Result<Core> {
    let (sender, _) = mpsc::unbounded_channel();
    let inner = Arc::new(DialogInner::new(
        TransactionRole::Server,
        id,
        tx.original.clone(),
        tx.endpoint_inner.clone(),
        sender,
        credential,
        Some(contact),
        transaction_event_sender_noop(),
    )?);
    crate::sip::routing::parse_routes(&inner.route_set.lock())?;
    inner.set_server_connection(tx.connection.clone());
    Ok(inner)
}
/// Registry counts include bounded fork discovery and terminal-response retention.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubscriptionDiagnostics {
    pub subscriptions: Vec<SubscriptionStatus>,
    pub initial_attempts: usize,
    pub retired_responders: usize,
    pub operations: Vec<PendingOperation>,
    pub admission_closed: bool,
}
struct Registry {
    attempts: BTreeMap<(String, String), Arc<Attempt>>,
    dialogs: BTreeMap<DialogId, SubscriptionDialog>,
    retired: BTreeMap<DialogId, SubscriptionDialog>,
}
#[derive(Clone)]
pub struct SubscriptionLayer {
    endpoint: EndpointInnerRef,
    control: Arc<control::Control>,
    registry: Arc<Mutex<Registry>>,
}
impl SubscriptionLayer {
    pub fn new(endpoint: EndpointInnerRef) -> Self {
        Self::with_control(endpoint, control::Control::new())
    }
    fn with_control(endpoint: EndpointInnerRef, control: Arc<control::Control>) -> Self {
        Self {
            endpoint,
            control,
            registry: Arc::new(Mutex::new(Registry {
                attempts: BTreeMap::new(),
                dialogs: BTreeMap::new(),
                retired: BTreeMap::new(),
            })),
        }
    }
    pub fn close_admission(&self) {
        self.control.close();
    }
    pub fn diagnostics(&self) -> SubscriptionDiagnostics {
        let (subscriptions, initial_attempts, retired_responders) = {
            let registry = self.registry.lock();
            (
                registry.dialogs.values().map(|d| d.status()).collect(),
                registry.attempts.len(),
                registry.retired.len(),
            )
        };
        SubscriptionDiagnostics {
            subscriptions,
            initial_attempts,
            retired_responders,
            operations: self.control.diagnostics().operations,
            admission_closed: self.control.closed(),
        }
    }
    pub fn pending_operations(&self) -> Vec<PendingOperation> {
        self.control.diagnostics().operations
    }
    pub async fn drain(&self, budget: Duration) -> DrainReport {
        self.control.drain(budget, || {}).await
    }
    pub fn dialogs(&self) -> Vec<SubscriptionStatus> {
        self.registry
            .lock()
            .dialogs
            .values()
            .map(|d| d.status())
            .collect()
    }
    pub fn get(&self, id: &DialogId) -> Option<SubscriptionDialog> {
        self.registry.lock().dialogs.get(id).cloned()
    }
    pub fn start_subscription(
        &self,
        options: SubscriptionOptions,
        events: SubscriptionEventSender,
    ) -> crate::Result<(SubscriptionAttempt, ExchangeOperation)> {
        let _gate = self.control.admit().map_err(|e| invalid(&e.to_string()))?;
        let from = crate::sip::typed::From {
            display_name: None,
            uri: options.subscriber.clone(),
            params: Vec::new(),
        }
        .with_tag(make_tag());
        let to = crate::sip::typed::To {
            display_name: None,
            uri: options.resource.clone(),
            params: Vec::new(),
        };
        let request = self.endpoint.make_request(
            Method::Subscribe,
            options.resource.clone(),
            self.endpoint.get_via(None, None)?,
            from,
            to,
            0,
            None,
        );
        let id = DialogId::try_from((&request, TransactionRole::Client))?;
        let (sender, _) = mpsc::unbounded_channel();
        let core = Arc::new(DialogInner::new(
            TransactionRole::Client,
            id.clone(),
            request,
            self.endpoint.clone(),
            sender,
            options.credential.clone(),
            Some(options.contact.clone()),
            transaction_event_sender_noop(),
        )?);
        crate::sip::routing::parse_routes(&options.route_set)?;
        *core.route_set.lock() = options.route_set.clone();
        let attempt = Arc::new(Attempt {
            core,
            options,
            events,
            sequence: Arc::new(Mutex::new(0)),
            branches: Mutex::new(Vec::new()),
            deadline: Mutex::new(None),
            settled: CancellationToken::new(),
        });
        self.registry
            .lock()
            .attempts
            .insert((id.call_id.clone(), id.local_tag.clone()), attempt.clone());
        let settled = attempt.settled.clone();
        self.control
            .track(Some(id.clone()), "subscription-establishment", async move {
                settled.cancelled().await;
            });
        let trace = Trace::new();
        let observer = trace.clone();
        let guard = DriverGuard(trace.clone());
        let pending = attempt.clone();
        let registry = Arc::downgrade(&self.registry);
        let tracking_id = self
            .control
            .track(Some(id.clone()), "subscribe-initial", async move {
                let _guard = guard;
                let result = async {
                    let expires = pending.options.expires;
                    let headers = subscription_headers(
                        &pending.options.event,
                        expires,
                        pending.options.accept.as_deref(),
                    );
                    let (headers, body) = match pending.options.body.clone() {
                        Some(b) => b.apply(Some(headers)),
                        None => (Some(headers), None),
                    };
                    let request = pending.core.make_request(
                        Method::Subscribe,
                        Some(next_cseq(&pending.sequence)?),
                        subscription_via(&pending.core, pending.options.destination.as_ref()),
                        None,
                        headers,
                        body,
                    )?;
                    let mut tx = pending.core.create_dialog_transaction(request, None)?;
                    tx.destination = pending.options.destination.clone();
                    let deadline = Instant::now() + pending.core.endpoint_inner.option.t1x64;
                    *pending.deadline.lock() = Some(deadline);
                    let weak = Arc::downgrade(&pending);
                    let registry = registry.clone();
                    let id = id.clone();
                    platform::spawn(async move {
                        loop {
                            let current = weak.upgrade().and_then(|a| *a.deadline.lock());
                            let Some(current) = current else {
                                return;
                            };
                            if current <= Instant::now() {
                                break;
                            }
                            platform::sleep(
                                current
                                    .checked_duration_since(Instant::now())
                                    .unwrap_or_default(),
                            )
                            .await;
                        }
                        if let Some(registry) = registry.upgrade() {
                            registry
                                .lock()
                                .attempts
                                .remove(&(id.call_id.clone(), id.local_tag.clone()));
                        }
                        if let Some(attempt) = weak.upgrade() {
                            attempt.settled.cancel();
                            if attempt.deadline.lock().is_some()
                                && attempt.branches.lock().is_empty()
                            {
                                let _ =
                                    attempt
                                        .events
                                        .send(SubscriptionEvent::EstablishmentTimedOut {
                                            call_id: id.call_id,
                                            local_tag: id.local_tag,
                                        });
                            }
                        }
                    });
                    Ok::<_, crate::Error>(tx)
                }
                .await;
                let result = match result {
                    Ok(tx) => {
                        exchange::send_observed(
                            tx,
                            pending.options.credential.as_ref(),
                            &trace,
                            || next_cseq(&pending.sequence),
                            |_| {
                                *pending.deadline.lock() =
                                    Some(Instant::now() + pending.core.endpoint_inner.option.t1x64)
                            },
                        )
                        .await
                    }
                    Err(error) => Err(exchange::protocol(error)),
                };
                let result = match result {
                    Ok(response) if (200..300).contains(&response.status_code.code()) => {
                        match exchange::header(&response, "Expires")
                            .and_then(|v| v.parse::<u32>().ok())
                        {
                            Some(expires) if expires <= pending.options.expires => Ok(()),
                            _ => Err(exchange::protocol("SUBSCRIBE success has invalid Expires")),
                        }
                    }
                    result => {
                        *pending.deadline.lock() = None;
                        pending.settled.cancel();
                        if let Some(registry) = registry.upgrade() {
                            registry.lock().attempts.remove(&(id.call_id, id.local_tag));
                        }
                        result.map(|_| ())
                    }
                };
                trace.finish(result);
            });
        Ok((
            SubscriptionAttempt(attempt),
            ExchangeOperation {
                trace: observer,
                tracking_id,
            },
        ))
    }
}
impl DialogLayer {
    /// Create an independent subscription registry sharing this layer's admission
    /// and operation accounting. Retain and clone the returned registry.
    pub fn subscriptions(&self) -> SubscriptionLayer {
        SubscriptionLayer::with_control(self.0.endpoint.clone(), self.1.clone())
    }
}
struct Attempt {
    core: Core,
    options: SubscriptionOptions,
    events: SubscriptionEventSender,
    sequence: Arc<Mutex<u32>>,
    branches: Mutex<Vec<SubscriptionDialog>>,
    deadline: Mutex<Option<Instant>>,
    settled: CancellationToken,
}
#[derive(Clone)]
pub struct SubscriptionAttempt(Arc<Attempt>);
impl SubscriptionAttempt {
    pub fn branches(&self) -> Vec<SubscriptionDialog> {
        self.0.branches.lock().clone()
    }
    pub fn waiting_notify(&self) -> bool {
        self.0.branches.lock().is_empty()
            && self.0.deadline.lock().is_some_and(|d| Instant::now() < d)
    }
}
struct Usage {
    phase: SubscriptionPhase,
    expires_at: Option<Instant>,
    notify_deadline: Option<Instant>,
    remote_seq: Option<u32>,
    applied_seq: u32,
    notification_count: u64,
    changed: CancellationToken,
}
struct Inner {
    core: Core,
    id: DialogId,
    event: SubscriptionEventType,
    role: SubscriptionRole,
    events: SubscriptionEventSender,
    sequence: Arc<Mutex<u32>>,
    state: Mutex<Usage>,
    queue: Queue,
    registry: Weak<Mutex<Registry>>,
    control: Arc<control::Control>,
    attempt: Mutex<Weak<Attempt>>,
    accept: Option<String>,
}
#[derive(Clone)]
pub struct SubscriptionDialog(Arc<Inner>);
impl SubscriptionDialog {
    pub fn id(&self) -> &DialogId {
        &self.0.id
    }
    pub fn status(&self) -> SubscriptionStatus {
        let state = self.0.state.lock();
        SubscriptionStatus {
            id: self.0.id.clone(),
            event: self.0.event.clone(),
            role: self.0.role,
            phase: state.phase.clone(),
            expires_at: state.expires_at,
            notify_deadline: state.notify_deadline,
            local_cseq: *self.0.sequence.lock(),
            remote_cseq: state.remote_seq,
        }
    }
    fn change(&self, change: impl FnOnce(&mut Usage)) {
        {
            let mut state = self.0.state.lock();
            change(&mut state);
            let old = core::mem::replace(&mut state.changed, CancellationToken::new());
            old.cancel();
        }
        let status = self.status();
        if matches!(status.phase, SubscriptionPhase::Terminated(_)) {
            if let Some(registry) = self.0.registry.upgrade() {
                let removed = registry.lock().dialogs.remove(self.id());
                if self.0.role == SubscriptionRole::Subscriber && removed.is_some() {
                    // A final NOTIFY can cross local expiry. Retain only a bounded
                    // terminal responder; it cannot reopen the usage or accept refreshes.
                    registry
                        .lock()
                        .retired
                        .insert(self.id().clone(), self.clone());
                    let weak = Arc::downgrade(&registry);
                    let id = self.id().clone();
                    let grace = self.0.core.endpoint_inner.option.t1x64;
                    platform::spawn(async move {
                        platform::sleep(grace).await;
                        if let Some(registry) = weak.upgrade() {
                            registry.lock().retired.remove(&id);
                        }
                    });
                }
            }
        }
        let _ = self.0.events.send(SubscriptionEvent::Changed(status));
    }
    fn terminate(&self, reason: &str) {
        self.change(|s| {
            if !matches!(s.phase, SubscriptionPhase::Terminated(_)) {
                s.phase = SubscriptionPhase::Terminated(reason.into());
            }
            s.notify_deadline = None;
            s.expires_at = None;
        });
    }
    fn check_live(&self) -> core::result::Result<(), AdmissionError> {
        if matches!(self.0.state.lock().phase, SubscriptionPhase::Terminated(_)) {
            Err(AdmissionError::InvalidRequest(
                "subscription has ended".into(),
            ))
        } else {
            Ok(())
        }
    }
    pub fn start_refresh(
        &self,
        expires: u32,
        body: Option<Body>,
    ) -> core::result::Result<ExchangeOperation, AdmissionError> {
        if expires == 0 {
            return self.start_unsubscribe();
        }
        self.start(Command::Subscribe(expires, body))
    }
    /// A 2xx response alone does not terminate the usage: wait for final NOTIFY.
    pub fn start_unsubscribe(&self) -> core::result::Result<ExchangeOperation, AdmissionError> {
        self.start(Command::Subscribe(0, None))
    }
    pub fn start_notify(
        &self,
        state: NotifyState,
        body: Option<Body>,
    ) -> core::result::Result<ExchangeOperation, AdmissionError> {
        state
            .validate()
            .map_err(|e| AdmissionError::InvalidRequest(e.to_string()))?;
        self.start(Command::Notify(state, body))
    }
    fn start(&self, command: Command) -> core::result::Result<ExchangeOperation, AdmissionError> {
        let cleanup = matches!(
            &command,
            Command::Subscribe(0, _) | Command::Notify(NotifyState::Terminated { .. }, _)
        );
        let _gate = if cleanup {
            None
        } else {
            Some(self.0.control.admit()?)
        };
        self.check_live()?;
        if self.0.role == SubscriptionRole::Notifier
            && self.0.state.lock().phase == SubscriptionPhase::WaitingNotify
        {
            return Err(AdmissionError::InvalidRequest(
                "accept the initial SUBSCRIBE before notifying".into(),
            ));
        }
        match (&command, self.0.role) {
            (Command::Subscribe(..), SubscriptionRole::Notifier)
            | (Command::Notify(..), SubscriptionRole::Subscriber) => {
                return Err(AdmissionError::InvalidRequest(
                    "operation belongs to the other subscription role".into(),
                ))
            }
            _ => {}
        }
        Ok(self.enqueue(command, None))
    }
    fn enqueue(&self, command: Command, after: Option<Arc<CommitGate>>) -> ExchangeOperation {
        self.enqueue_inner(command, after, false)
    }
    fn enqueue_inner(
        &self,
        command: Command,
        after: Option<Arc<CommitGate>>,
        expiry_notification: bool,
    ) -> ExchangeOperation {
        let ticket = self.0.queue.reserve();
        let trace = Trace::new();
        let observer = trace.clone();
        let guard = DriverGuard(trace.clone());
        let dialog = self.clone();
        let tracking_id = self.0.control.track(
            Some(self.0.id.clone()),
            "subscription-exchange",
            async move {
                let _guard = guard;
                ticket.wait().await;
                let result = async {
                    if let Some(gate) = after {
                        gate.done.cancelled().await;
                        if !*gate.accepted.lock() {
                            return Err(exchange::protocol("SUBSCRIBE response was not sent"));
                        }
                    }
                    if !expiry_notification {
                        dialog.check_live().map_err(exchange::protocol)?;
                    }
                    dialog.drive(command, &trace).await
                }
                .await;
                trace.finish(result);
                drop(ticket);
            },
        );
        ExchangeOperation {
            trace: observer,
            tracking_id,
        }
    }
    async fn drive(
        &self,
        command: Command,
        trace: &Trace,
    ) -> core::result::Result<(), OperationError> {
        let (method, headers, body, terminal, subscribed) = match command {
            Command::Subscribe(expires, body) => {
                self.change(|s| {
                    s.notify_deadline =
                        Some(Instant::now() + self.0.core.endpoint_inner.option.t1x64)
                });
                (
                    Method::Subscribe,
                    subscription_headers(&self.0.event, expires, self.0.accept.as_deref()),
                    body,
                    false,
                    Some(expires),
                )
            }
            Command::Notify(state, body) => {
                let expires = self
                    .0
                    .state
                    .lock()
                    .expires_at
                    .map(|t| {
                        let remaining =
                            t.checked_duration_since(Instant::now()).unwrap_or_default();
                        (remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0))
                            .min(u32::MAX as u64) as u32
                    })
                    .unwrap_or(0);
                let terminal = matches!(state, NotifyState::Terminated { .. });
                let headers = vec![
                    Header::Event(self.0.event.wire().into()),
                    Header::SubscriptionState(state.wire(expires).into()),
                ];
                self.change(|s| s.phase = state.phase());
                (Method::Notify, headers, body, terminal, None)
            }
        };
        if subscribed.is_some() {
            let dialog = self.clone();
            self.0.control.track(
                Some(self.id().clone()),
                "subscription-notify-wait",
                async move {
                    loop {
                        let changed = {
                            let state = dialog.0.state.lock();
                            if state.notify_deadline.is_none()
                                || matches!(state.phase, SubscriptionPhase::Terminated(_))
                            {
                                break;
                            }
                            state.changed.clone()
                        };
                        changed.cancelled().await;
                    }
                },
            );
        }
        let mut notifications = self.0.state.lock().notification_count;
        let (headers, body) = match body {
            Some(body) => body.apply(Some(headers)),
            None => (Some(headers), None),
        };
        let request = self
            .0
            .core
            .make_request(
                method,
                Some(next_cseq(&self.0.sequence).map_err(exchange::protocol)?),
                None,
                None,
                headers,
                body,
            )
            .map_err(exchange::protocol)?;
        let tx = self
            .0
            .core
            .create_dialog_transaction(request, None)
            .map_err(exchange::protocol)?;
        let result = exchange::send_observed(
            tx,
            self.0.core.credential.as_ref(),
            trace,
            || next_cseq(&self.0.sequence),
            |_| {
                if subscribed.is_some() {
                    notifications = self.0.state.lock().notification_count;
                    self.change(|s| {
                        s.notify_deadline =
                            Some(Instant::now() + self.0.core.endpoint_inner.option.t1x64)
                    });
                }
            },
        )
        .await;
        if terminal {
            self.terminate("local termination");
        }
        match result {
            Ok(response) => {
                if DialogId::try_from((&response, TransactionRole::Client))
                    .map_err(exchange::protocol)?
                    != self.0.id
                {
                    return Err(exchange::protocol("response dialog identity mismatch"));
                }
                let code = response.status_code.code();
                if [
                    404, 405, 410, 416, 480, 481, 482, 483, 484, 485, 489, 501, 604,
                ]
                .contains(&code)
                    || (subscribed.is_none() && [408, 500, 503].contains(&code))
                {
                    self.terminate("peer rejected subscription usage");
                }
                if let Some(expires) = subscribed {
                    if (200..300).contains(&code) {
                        let granted: u32 = exchange::header(&response, "Expires")
                            .ok_or_else(|| exchange::protocol("SUBSCRIBE success lacks Expires"))?
                            .parse()
                            .map_err(exchange::protocol)?;
                        if granted > expires {
                            return Err(exchange::protocol("SUBSCRIBE response lengthens expiry"));
                        }
                        let target = contact(&response).map_err(exchange::protocol)?;
                        self.change(|s| {
                            if s.notification_count == notifications
                                && !matches!(s.phase, SubscriptionPhase::Terminated(_))
                            {
                                if granted > 0 {
                                    s.expires_at =
                                        Some(Instant::now() + Duration::from_secs(granted.into()));
                                }
                                *self.0.core.remote_uri.lock() = target;
                            }
                        });
                    }
                }
                Ok(())
            }
            Err(error) => {
                if subscribed.is_none() {
                    self.terminate("NOTIFY transaction failed");
                }
                Err(error)
            }
        }
    }
}
enum Command {
    Subscribe(u32, Option<Body>),
    Notify(NotifyState, Option<Body>),
}
fn subscription_headers(
    event: &SubscriptionEventType,
    expires: u32,
    accept: Option<&str>,
) -> Vec<Header> {
    let mut headers = vec![
        Header::Event(event.wire().into()),
        Header::Expires(expires.into()),
    ];
    if let Some(accept) = accept {
        headers.push(Header::Accept(accept.into()));
    }
    headers
}

/// Validation failure retains the transaction so the application can send the
/// suggested final status through the transaction response-receipt API.
pub struct SubscriptionRejection {
    pub transaction: Transaction,
    pub status: StatusCode,
    pub reason: String,
}
impl core::fmt::Debug for SubscriptionRejection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SubscriptionRejection")
            .field("status", &self.status)
            .field("reason", &self.reason)
            .finish()
    }
}
impl core::fmt::Display for SubscriptionRejection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}: {}", self.status, self.reason)
    }
}
impl core::error::Error for SubscriptionRejection {}
fn reject(transaction: Transaction, status: u16, reason: impl ToString) -> SubscriptionRejection {
    SubscriptionRejection {
        transaction,
        status: StatusCode::from(status),
        reason: reason.to_string(),
    }
}

impl SubscriptionLayer {
    fn branch(
        &self,
        core: Core,
        event: SubscriptionEventType,
        role: SubscriptionRole,
        events: SubscriptionEventSender,
        sequence: Arc<Mutex<u32>>,
        accept: Option<String>,
    ) -> SubscriptionDialog {
        let id = core.id.lock().clone();
        let dialog = SubscriptionDialog(Arc::new(Inner {
            core,
            id,
            event,
            role,
            events,
            sequence,
            queue: Queue::new(),
            attempt: Mutex::new(Weak::new()),
            accept,
            registry: Arc::downgrade(&self.registry),
            control: self.control.clone(),
            state: Mutex::new(Usage {
                phase: SubscriptionPhase::WaitingNotify,
                expires_at: None,
                notify_deadline: Some(Instant::now() + self.endpoint.option.t1x64),
                remote_seq: None,
                applied_seq: 0,
                notification_count: 0,
                changed: CancellationToken::new(),
            }),
        }));
        spawn_expiry(&dialog);
        dialog
    }
    /// Synchronous matching and CSeq admission must run in wire arrival order.
    /// This handles NOTIFY before the initial SUBSCRIBE response and forked tags.
    pub fn receive_notify(
        &self,
        transaction: Transaction,
    ) -> core::result::Result<IncomingNotify, SubscriptionRejection> {
        if transaction.role() != TransactionRole::Server
            || transaction.original.method != Method::Notify
        {
            return Err(reject(transaction, 400, "expected incoming NOTIFY"));
        }
        let validation: crate::Result<_> = (|| {
            let id = DialogId::try_from((&transaction.original, TransactionRole::Server))?;
            if id.local_tag.is_empty() || id.remote_tag.is_empty() {
                return Err(invalid("NOTIFY requires both tags"));
            }
            Ok((
                id,
                event(&transaction.original)?,
                parse_notify(&transaction.original)?,
                contact(&transaction.original)?,
                transaction.original.cseq_header()?.seq()?,
            ))
        })();
        let (id, event, notify, target, seq) = match validation {
            Ok(v) => v,
            Err(e) => return Err(reject(transaction, 400, e)),
        };
        let mut registry = self.registry.lock();
        let dialog = if let Some(dialog) = registry.dialogs.get(&id) {
            dialog.clone()
        } else if let Some(dialog) = registry
            .retired
            .get(&id)
            .filter(|_| matches!(notify.phase, SubscriptionPhase::Terminated(_)))
        {
            dialog.clone()
        } else {
            let attempt = registry
                .attempts
                .get(&(id.call_id.clone(), id.local_tag.clone()))
                .cloned();
            let Some(attempt) = attempt.filter(|a| {
                a.options.event == event && a.deadline.lock().is_some_and(|d| Instant::now() < d)
            }) else {
                return Err(reject(transaction, 481, "no matching subscription"));
            };
            // A terminated/rejected fork cannot be resurrected during Timer N.
            if attempt.branches.lock().iter().any(|d| d.id() == &id) {
                return Err(reject(transaction, 481, "subscription branch ended"));
            }
            let core = match core_for(
                &transaction,
                id.clone(),
                attempt.options.contact.clone(),
                attempt.options.credential.clone(),
            ) {
                Ok(core) => core,
                Err(e) => return Err(reject(transaction, 400, e)),
            };
            let dialog = self.branch(
                core,
                event.clone(),
                SubscriptionRole::Subscriber,
                attempt.events.clone(),
                attempt.sequence.clone(),
                attempt.options.accept.clone(),
            );
            *dialog.0.attempt.lock() = Arc::downgrade(&attempt);
            registry.dialogs.insert(id.clone(), dialog.clone());
            attempt.branches.lock().push(dialog.clone());
            dialog
        };
        drop(registry);
        if dialog.0.role != SubscriptionRole::Subscriber || dialog.0.event != event {
            return Err(reject(
                transaction,
                481,
                "subscription Event or role mismatch",
            ));
        }
        if !admit_remote(
            &dialog,
            seq,
            matches!(notify.phase, SubscriptionPhase::Terminated(_)),
        ) {
            return Err(reject(transaction, 500, "out-of-order CSeq"));
        }
        let responder = Responder::new(transaction, dialog.clone(), false);
        Ok(IncomingNotify {
            responder,
            dialog,
            notify: Arc::new(notify),
            target,
            seq,
        })
    }
    /// Receive an initial SUBSCRIBE or refresh. Authorization and event package
    /// policy run before start_accept; accepting also queues the mandatory NOTIFY.
    pub fn receive_subscribe(
        &self,
        transaction: Transaction,
        options: NotifierOptions,
        events: SubscriptionEventSender,
    ) -> core::result::Result<IncomingSubscription, SubscriptionRejection> {
        if transaction.role() != TransactionRole::Server
            || transaction.original.method != Method::Subscribe
        {
            return Err(reject(transaction, 400, "expected incoming SUBSCRIBE"));
        }
        let validation: crate::Result<_> = (|| {
            let id = DialogId::try_from((&transaction.original, TransactionRole::Server))?;
            let expiry = match exchange::header(&transaction.original, "Expires") {
                Some(v) => v.parse().map_err(|_| invalid("invalid Expires"))?,
                None => options.default_expires,
            };
            Ok((
                id,
                event(&transaction.original)?,
                contact(&transaction.original)?,
                transaction.original.cseq_header()?.seq()?,
                expiry,
            ))
        })();
        let (mut id, event, target, seq, requested) = match validation {
            Ok(v) => v,
            Err(e) => return Err(reject(transaction, 400, e)),
        };
        let initial = id.local_tag.is_empty();
        let dialog = if initial {
            let _gate = match self.control.admit() {
                Ok(g) => g,
                Err(e) => return Err(reject(transaction, 503, e)),
            };
            id.local_tag = make_tag().value().to_string();
            let core = match core_for(
                &transaction,
                id.clone(),
                options.contact,
                options.credential,
            ) {
                Ok(c) => c,
                Err(e) => return Err(reject(transaction, 400, e)),
            };
            let dialog = self.branch(
                core,
                event.clone(),
                SubscriptionRole::Notifier,
                events,
                Arc::new(Mutex::new(0)),
                None,
            );
            self.registry.lock().dialogs.insert(id, dialog.clone());
            dialog
        } else {
            match self.get(&id) {
                Some(dialog) => dialog,
                None => return Err(reject(transaction, 481, "no matching subscription")),
            }
        };
        if dialog.0.role != SubscriptionRole::Notifier || dialog.0.event != event {
            return Err(reject(
                transaction,
                481,
                "subscription Event or role mismatch",
            ));
        }
        if !admit_remote(&dialog, seq, false) {
            return Err(reject(transaction, 500, "out-of-order CSeq"));
        }
        let responder = Responder::new(transaction, dialog.clone(), initial);
        Ok(IncomingSubscription {
            responder,
            dialog,
            requested,
            target,
            seq,
        })
    }
}
fn admit_remote(dialog: &SubscriptionDialog, seq: u32, terminal_notify: bool) -> bool {
    let mut state = dialog.0.state.lock();
    if state.remote_seq.is_some_and(|previous| seq <= previous)
        || seq >= 1 << 31
        || (!terminal_notify && matches!(state.phase, SubscriptionPhase::Terminated(_)))
    {
        return false;
    }
    state.remote_seq = Some(seq);
    true
}
struct CommitGate {
    done: CancellationToken,
    accepted: Mutex<bool>,
}
struct CommitOwner(Arc<CommitGate>);
impl Drop for CommitOwner {
    fn drop(&mut self) {
        self.0.done.cancel();
    }
}
struct InboundCommand {
    submission: crate::dialog::response_operation::ResponseSubmission,
    action: Option<Accepted>,
    gate: Option<CommitOwner>,
}
enum Accepted {
    Notify {
        phase: SubscriptionPhase,
        expires: Option<u32>,
        target: Uri,
        seq: u32,
    },
    Subscribe {
        phase: SubscriptionPhase,
        expires: u32,
        target: Uri,
        seq: u32,
    },
}
#[derive(Clone)]
struct Responder {
    request: Request,
    dialog: SubscriptionDialog,
    sender: mpsc::UnboundedSender<InboundCommand>,
    admitted: Arc<Mutex<bool>>,
}
impl Responder {
    #[cfg_attr(not(feature = "platform-tokio"), allow(unused_mut))]
    fn new(mut transaction: Transaction, dialog: SubscriptionDialog, initial: bool) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<InboundCommand>();
        let responder = Self {
            request: transaction.original.clone(),
            dialog: dialog.clone(),
            sender,
            admitted: Arc::new(Mutex::new(false)),
        };
        let control = dialog.0.control.clone();
        control.track(
            Some(dialog.id().clone()),
            "incoming-subscription",
            async move {
                let command =
                    platform::timeout(transaction.endpoint_inner.option.t1x64, receiver.recv())
                        .await;
                match command {
                    Ok(Some(command)) => {
                        let success =
                            (200..300).contains(&command.submission.response.status_code.code());
                        let sent = transaction
                            .respond_submission(command.submission)
                            .await
                            .is_ok();
                        if success && sent {
                            if let Some(action) = command.action {
                                dialog.change(|s| {
                                    let (phase, expires, target, seq, is_notify) = match action {
                                        Accepted::Notify {
                                            phase,
                                            expires,
                                            target,
                                            seq,
                                        } => (phase, expires, target, seq, true),
                                        Accepted::Subscribe {
                                            phase,
                                            expires,
                                            target,
                                            seq,
                                        } => (phase, Some(expires), target, seq, false),
                                    };
                                    if seq >= s.applied_seq
                                        && !matches!(s.phase, SubscriptionPhase::Terminated(_))
                                    {
                                        s.applied_seq = seq;
                                        s.phase = phase;
                                        s.notify_deadline = None;
                                        if let Some(expires) = expires {
                                            s.expires_at = if expires == 0 && !is_notify {
                                                None
                                            } else {
                                                Some(
                                                    Instant::now()
                                                        + Duration::from_secs(expires.into()),
                                                )
                                            };
                                        }
                                        if is_notify {
                                            s.notification_count += 1;
                                        }
                                        *dialog.0.core.remote_uri.lock() = target;
                                    }
                                });
                            }
                            if dialog.0.role == SubscriptionRole::Subscriber {
                                if let Some(attempt) = dialog.0.attempt.lock().upgrade() {
                                    attempt.settled.cancel();
                                }
                            }
                            if let Some(gate) = &command.gate {
                                *gate.0.accepted.lock() = true;
                            }
                        } else if initial || dialog.0.role == SubscriptionRole::Subscriber {
                            dialog.terminate("incoming subscription request rejected");
                        }
                        // Release the NOTIFY only after usage and routing state committed.
                        drop(command.gate);
                    }
                    _ => {
                        let _ = transaction.reply(StatusCode::NotImplemented).await;
                        if initial || dialog.0.role == SubscriptionRole::Subscriber {
                            dialog.terminate("responder dropped or timed out");
                        }
                    }
                }
                while transaction.receive().await.is_some() {}
            },
        );
        responder
    }
    fn submit(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        action: Option<Accepted>,
        gate: Option<CommitOwner>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        if !(200..700).contains(&status.code()) {
            return Err(ResponseError::Protocol("expected final response".into()));
        }
        let mut admitted = self.admitted.lock();
        if *admitted {
            return Err(ResponseError::FinalAlreadyAdmitted);
        }
        if headers.as_ref().is_some_and(|headers| {
            headers.iter().any(|h| {
                [
                    "via",
                    "v",
                    "from",
                    "f",
                    "to",
                    "t",
                    "call-id",
                    "i",
                    "cseq",
                    "contact",
                    "m",
                    "record-route",
                    "content-length",
                    "l",
                ]
                .iter()
                .any(|n| h.name().eq_ignore_ascii_case(n))
            })
        }) {
            return Err(ResponseError::Protocol(
                "response identity, routing and framing are stack-owned".into(),
            ));
        }
        let response = self
            .dialog
            .0
            .core
            .make_response(&self.request, status, headers, None);
        let (receipt, producer) = ResponseReceipt::pair();
        producer
            .trace()
            .acknowledge(Err(ResponseError::AckNotApplicable));
        self.sender
            .send(InboundCommand {
                submission: crate::dialog::response_operation::ResponseSubmission {
                    response,
                    producer: Some(producer),
                    initial_owner: None,
                },
                action,
                gate,
            })
            .map_err(|_| ResponseError::Closed)?;
        *admitted = true;
        Ok(receipt)
    }
}
#[derive(Clone)]
pub struct IncomingNotify {
    responder: Responder,
    dialog: SubscriptionDialog,
    notify: Arc<ParsedNotify>,
    target: Uri,
    seq: u32,
}
impl IncomingNotify {
    pub fn request(&self) -> &Request {
        &self.responder.request
    }
    pub fn dialog(&self) -> &SubscriptionDialog {
        &self.dialog
    }
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        let action = (200..300)
            .contains(&status.code())
            .then(|| Accepted::Notify {
                phase: self.notify.phase.clone(),
                expires: self.notify.expires,
                target: self.target.clone(),
                seq: self.seq,
            });
        self.responder.submit(status, headers, action, None)
    }
}
#[derive(Clone)]
pub struct IncomingSubscription {
    responder: Responder,
    dialog: SubscriptionDialog,
    requested: u32,
    target: Uri,
    seq: u32,
}
#[must_use]
pub struct SubscriptionAcceptance {
    pub response: ResponseReceipt,
    pub notification: ExchangeOperation,
}
impl IncomingSubscription {
    pub fn request(&self) -> &Request {
        &self.responder.request
    }
    pub fn dialog(&self) -> &SubscriptionDialog {
        &self.dialog
    }
    pub fn requested_expires(&self) -> u32 {
        self.requested
    }
    /// Accept and immediately notify. Expires=0 always produces a terminal NOTIFY.
    pub fn start_accept(
        &self,
        expires: u32,
        state: NotifyState,
        body: Option<Body>,
    ) -> core::result::Result<SubscriptionAcceptance, ResponseError> {
        self.dialog
            .check_live()
            .map_err(|e| ResponseError::Protocol(e.to_string()))?;
        state
            .validate()
            .map_err(|e| ResponseError::Protocol(e.to_string()))?;
        if expires > self.requested {
            return Err(ResponseError::Protocol(
                "cannot lengthen requested expiry".into(),
            ));
        }
        let state = if expires == 0 {
            NotifyState::terminated("timeout")
        } else {
            state
        };
        let gate = Arc::new(CommitGate {
            done: CancellationToken::new(),
            accepted: Mutex::new(false),
        });
        let response = self.responder.submit(
            StatusCode::OK,
            Some(vec![Header::Expires(expires.into())]),
            Some(Accepted::Subscribe {
                phase: SubscriptionPhase::Pending,
                expires,
                target: self.target.clone(),
                seq: self.seq,
            }),
            Some(CommitOwner(gate.clone())),
        )?;
        let notification = self
            .dialog
            .enqueue(Command::Notify(state, body), Some(gate));
        Ok(SubscriptionAcceptance {
            response,
            notification,
        })
    }
    /// Success requires start_accept so the mandatory NOTIFY cannot be forgotten.
    pub fn start_reject(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        if status.code() < 300 {
            return Err(ResponseError::Protocol(
                "use start_accept for success".into(),
            ));
        }
        self.responder.submit(status, headers, None, None)
    }
}
fn spawn_expiry(dialog: &SubscriptionDialog) {
    let weak = Arc::downgrade(&dialog.0);
    platform::spawn(async move {
        loop {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let (deadline, changed, ended) = {
                let state = inner.state.lock();
                (
                    match (state.expires_at, state.notify_deadline) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    },
                    state.changed.clone(),
                    matches!(state.phase, SubscriptionPhase::Terminated(_)),
                )
            };
            if ended {
                return;
            }
            let Some(deadline) = deadline else {
                drop(inner);
                changed.cancelled().await;
                continue;
            };
            if Instant::now() >= deadline {
                let notify = {
                    let mut state = inner.state.lock();
                    let current = match (state.expires_at, state.notify_deadline) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    // Refresh and expiry arbitrate under the same usage lock.
                    if current != Some(deadline) || changed.is_cancelled() {
                        continue;
                    }
                    if matches!(state.phase, SubscriptionPhase::Terminated(_)) {
                        return;
                    }
                    let notify = inner.role == SubscriptionRole::Notifier
                        && matches!(
                            state.phase,
                            SubscriptionPhase::Active | SubscriptionPhase::Pending
                        );
                    state.phase = SubscriptionPhase::Terminated(
                        "subscription or NOTIFY deadline expired".into(),
                    );
                    state.expires_at = None;
                    state.notify_deadline = None;
                    notify
                };
                let dialog = SubscriptionDialog(inner);
                dialog.change(|_| {});
                if notify {
                    // Expiry already closed the usage; only its final notification
                    // bypasses the live-usage check, never an application command.
                    let _ = dialog.enqueue_inner(
                        Command::Notify(NotifyState::terminated("timeout"), None),
                        None,
                        true,
                    );
                }
                return;
            }
            drop(inner);
            platform::select2(
                core::pin::pin!(platform::sleep(
                    deadline
                        .checked_duration_since(Instant::now())
                        .unwrap_or_default()
                )),
                core::pin::pin!(changed.cancelled()),
            )
            .await;
        }
    });
}

fn subscription_via(
    core: &Core,
    destination: Option<&crate::transport::SipAddr>,
) -> Option<crate::transport::SipAddr> {
    use crate::sip::uri::ParamsExt;
    let target = core.remote_uri.lock().clone();
    let hop = crate::sip::routing::next_hop(&target, &core.route_set.lock()).ok()?;
    let transport = destination
        .and_then(|d| d.r#type)
        .or_else(|| hop.transport().cloned())
        .or_else(|| {
            (hop.scheme == Some(crate::sip::Scheme::Sips)).then_some(crate::sip::Transport::Tls)
        })?;
    let listeners = core.endpoint_inner.transport_layer.get_listen_addrs();
    listeners
        .iter()
        .find(|a| a.r#type.unwrap_or_default() == transport)
        .cloned()
        .or_else(|| {
            let mut addr = listeners.first()?.clone();
            addr.r#type = Some(transport);
            Some(addr)
        })
}

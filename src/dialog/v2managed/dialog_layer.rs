use super::*;
use crate::{sip::Uri, transaction::endpoint::EndpointInnerRef};

/// A registry owned by this API. It cannot share live handles with a legacy layer.
pub struct DialogLayer(
    pub(super) Arc<crate::dialog::dialog_layer::DialogLayer>,
    pub(super) Arc<control::Control>,
);
impl DialogLayer {
    pub fn new(endpoint: EndpointInnerRef) -> Self {
        Self(
            Arc::new(crate::dialog::dialog_layer::DialogLayer::new(endpoint)),
            control::Control::new(),
        )
    }
    pub fn new_dialog_event_channel(&self) -> (DialogEventSender, DialogEventReceiver) {
        dialog_event_channel()
    }
    pub fn set_lifecycle_events(&self, sender: Option<DialogLifecycleSender>) {
        self.0.set_lifecycle_events(sender);
    }
    pub fn set_persistence(&self, config: Option<DialogPersistenceConfig>) -> crate::Result<()> {
        self.0.set_persistence(config)
    }
    pub fn set_reliable_provisional(&self, config: Option<ReliableProvisionalConfig>) {
        self.0.set_reliable_provisional(config);
    }
    pub fn set_session_timer(&self, config: Option<SessionTimerConfig>) -> crate::Result<()> {
        self.0.set_session_timer(config)
    }
    pub fn start_invite(
        &self,
        options: impl Into<InviteOption>,
        states: DialogEventSender,
    ) -> crate::Result<(InviteDialog, InitialInviteOperation)> {
        let _gate = self
            .1
            .admit()
            .map_err(|e| crate::Error::Error(e.to_string()))?;
        let (dialog, operation) = self.0.start_invite(options.into(), states.0)?;
        let waiter = operation.clone();
        self.1
            .track(Some(dialog.id()), "initial-invite", async move {
                waiter.wait().await;
            });
        Ok((
            InviteDialog::new(dialog, self.1.clone()),
            InitialInviteOperation(operation, self.1.clone()),
        ))
    }
    pub fn start_incoming_invite(
        &self,
        transaction: Transaction,
        states: DialogEventSender,
        contact: Option<Uri>,
    ) -> core::result::Result<IncomingInvite, IncomingInviteError> {
        let _gate = match self.1.admit() {
            Ok(gate) => gate,
            Err(error) => {
                return Err(IncomingInviteError {
                    error: crate::Error::Error(error.to_string()),
                    transaction: Some(transaction),
                })
            }
        };
        let incoming = self
            .0
            .start_incoming_invite(transaction, states.0, contact)
            .map_err(|error| IncomingInviteError {
                error,
                transaction: None,
            })?;
        self.1.register(&incoming.dialog());
        let waiter = incoming.clone();
        self.1.track(
            Some(incoming.dialog().id()),
            "incoming-invite",
            async move {
                let _ = waiter.wait().await;
            },
        );
        Ok(IncomingInvite(incoming, self.1.clone()))
    }
    pub fn start_recovery_termination(
        &self,
        snapshot: DialogSnapshot,
        states: DialogEventSender,
        credential: Option<Credential>,
        headers: Option<Vec<Header>>,
    ) -> crate::Result<DialogOperation> {
        let operation = self
            .0
            .start_recovery_termination(snapshot, states.0, credential, headers)?;
        let waiter = operation.clone();
        self.1.track(
            Some(operation.id().dialog().clone()),
            "recovery-termination",
            async move {
                waiter.wait().await;
            },
        );
        Ok(DialogOperation(operation))
    }
    pub fn restore_from_snapshot_detailed(
        &self,
        snapshot: DialogSnapshot,
        states: DialogEventSender,
        credential: Option<Credential>,
    ) -> core::result::Result<RestoreOutcome, RestoreError> {
        let _gate = self
            .1
            .admit()
            .map_err(|e| RestoreError::Initialization(e.to_string()))?;
        let result = self
            .0
            .restore_from_snapshot_detailed(snapshot, states.0, credential)?;
        if let RestoreOutcome::Restored(id) | RestoreOutcome::AlreadyPresent(id) = &result {
            self.get_dialog(id);
        }
        Ok(result)
    }
    pub fn get_dialog(&self, id: &DialogId) -> Option<Dialog> {
        self.0
            .get_dialog(id)
            .and_then(|d| Dialog::from_core(d, self.1.clone()))
    }
    pub fn match_dialog(&self, tx: &Transaction) -> Option<Dialog> {
        self.0
            .match_dialog(tx)
            .and_then(|d| Dialog::from_core(d, self.1.clone()))
    }
    pub fn get_client_dialog_by_call_id(&self, call_id: &str) -> Vec<InviteDialog> {
        self.0
            .get_client_dialog_by_call_id(call_id)
            .into_iter()
            .map(|d| InviteDialog::new(d, self.1.clone()))
            .collect()
    }
    pub fn remove_dialog(&self, id: &DialogId) {
        self.0.remove_dialog(id);
    }
    /// Permanently close admission of new application requests and initial calls.
    /// Incoming in-dialog signaling, responses, CANCEL and explicit termination
    /// remain available. This does not stop the endpoint or its session timers.
    pub fn close_admission(&self) {
        self.1.close();
    }
    pub fn is_admission_closed(&self) -> bool {
        self.1.closed()
    }
    pub fn diagnostics(&self) -> LayerDiagnostics {
        self.register_dialogs();
        self.1.diagnostics()
    }
    fn register_dialogs(&self) {
        for id in self.dialog_ids() {
            self.get_dialog(&id);
        }
    }
    /// Wait for accepted work, pending SIP signaling and persistence cutoffs.
    /// Active quiet calls remain active. Peer traffic and timer refreshes can
    /// add work while waiting. A successful report is an observation of quiet
    /// signaling, not endpoint retirement or a promise against future traffic.
    /// Timeout never cancels signaling. Do not call from a storage callback.
    pub async fn drain(&self, budget: core::time::Duration) -> DrainReport {
        self.register_dialogs();
        self.1.drain(budget, || self.register_dialogs()).await
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn dialog_ids(&self) -> Vec<DialogId> {
        self.0.dialog_ids()
    }
}

/// Managed INVITE usages. Other dialog usages have not acquired an owned API yet.
#[derive(Clone)]
#[non_exhaustive]
pub enum Dialog {
    Invite(InviteDialog),
}
impl Dialog {
    fn from_core(
        dialog: crate::dialog::dialog::Dialog,
        control: Arc<control::Control>,
    ) -> Option<Self> {
        match dialog {
            crate::dialog::dialog::Dialog::Invite(dialog) => {
                Some(Self::Invite(InviteDialog::new(dialog, control)))
            }
            _ => None,
        }
    }
    pub fn id(&self) -> DialogId {
        match self {
            Self::Invite(d) => d.id(),
        }
    }
    pub fn state(&self) -> DialogState {
        match self {
            Self::Invite(d) => d.state(),
        }
    }
    pub fn snapshot(&self) -> DialogSnapshot {
        match self {
            Self::Invite(d) => d.snapshot(),
        }
    }
    pub fn start_incoming_request(
        &self,
        tx: Transaction,
    ) -> crate::Result<IncomingRequestOperation> {
        match self {
            Self::Invite(d) => d.start_incoming_request(tx),
        }
    }
}

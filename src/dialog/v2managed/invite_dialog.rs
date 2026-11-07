use super::*;
/// An INVITE session created or restored by a managed layer.
#[derive(Clone)]
pub struct InviteDialog(
    pub(super) crate::dialog::invite_dialog::InviteDialog,
    pub(super) Arc<control::Control>,
);
impl InviteDialog {
    pub(super) fn new(
        dialog: crate::dialog::invite_dialog::InviteDialog,
        control: Arc<control::Control>,
    ) -> Self {
        control.register(&dialog);
        Self(dialog, control)
    }
    pub fn diagnostics(&self) -> DialogDiagnostics {
        control::dialog_diagnostics(&self.0)
    }
    fn observe(
        &self,
        operation: crate::dialog::operation::DialogOperation,
        kind: &'static str,
    ) -> DialogOperation {
        let waiter = operation.clone();
        self.1.track(Some(self.id()), kind, async move {
            waiter.wait().await;
        });
        DialogOperation(operation)
    }

    pub fn id(&self) -> DialogId {
        self.0.id()
    }
    pub fn role(&self) -> TransactionRole {
        self.0.role()
    }
    pub fn state(&self) -> DialogState {
        DialogState::from_core(self.0.state())
    }
    pub fn initial_request(&self) -> Request {
        self.0.initial_request()
    }
    pub fn last_remote_ack(&self) -> Option<Request> {
        self.0.last_remote_ack()
    }
    pub fn snapshot(&self) -> DialogSnapshot {
        self.0.snapshot()
    }
    pub fn persistence_status(&self) -> Option<DialogPersistenceStatus> {
        self.0.persistence_status()
    }
    pub fn persistence_barrier(&self) -> PersistenceBarrier {
        self.0.persistence_barrier()
    }
    pub fn session_timer_status(&self) -> Option<SessionTimerStatus> {
        self.0.session_timer_status()
    }
    pub fn start_request(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        // BYE is explicit cleanup and remains available after closing admission.
        let _gate = if method == Method::Bye {
            None
        } else {
            Some(self.1.admit()?)
        };
        let operation = self.0.admit_request(method, headers, body)?;
        Ok(self.observe(operation, "request"))
    }
    pub fn start_reinvite(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        options: ReinviteOptions,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let _gate = self.1.admit()?;
        let operation = self.0.admit_reinvite(headers, body, options)?;
        Ok(self.observe(operation, "reinvite"))
    }
    pub fn start_late_answer_cleanup(
        &self,
        completion: &OperationCompletion,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let operation = self
            .0
            .admit_late_answer_cleanup(&completion.inner, headers)?;
        Ok(self.observe(operation, "late-answer-cleanup"))
    }
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        let receipt = self.0.start_response(status, headers, body)?;
        let waiter = receipt.clone();
        self.1.track(Some(self.id()), "response", async move {
            if waiter.wait_sent().await.is_ok() {
                let _ = waiter.wait_ack().await;
            }
        });
        Ok(receipt)
    }
    pub fn start_incoming_request(
        &self,
        transaction: Transaction,
    ) -> crate::Result<IncomingRequestOperation> {
        let operation = self.0.start_incoming_request(transaction)?;
        let waiter = operation.clone();
        self.1
            .track(Some(self.id()), "incoming-request", async move {
                let _ = waiter.wait().await;
            });
        Ok(operation)
    }
    pub fn start_cancel(&self) -> crate::Result<CancelOperation> {
        let operation = self.0.start_cancel()?;
        let waiter = operation.clone();
        self.1.track(Some(self.id()), "cancel", async move {
            waiter.wait().await;
        });
        Ok(operation)
    }
    pub fn start_termination(
        &self,
        options: TerminationOptions,
    ) -> core::result::Result<TerminationOperation, AdmissionError> {
        let operation = self.0.admit_termination(options)?;
        let waiter = operation.clone();
        self.1.track(Some(self.id()), "termination", async move {
            waiter.wait().await;
        });
        Ok(TerminationOperation(operation))
    }
}

impl InviteDialog {
    pub fn start_request_body(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Body,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let (headers, body) = body.apply(headers);
        self.start_request(method, headers, body)
    }
    pub fn start_reinvite_body(
        &self,
        headers: Option<Vec<Header>>,
        body: Body,
        options: ReinviteOptions,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let (headers, body) = body.apply(headers);
        self.start_reinvite(headers, body, options)
    }
    pub fn start_response_body(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Body,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        let (headers, body) = body.apply(headers);
        self.start_response(status, headers, body)
    }
}

impl InviteDialog {
    /// Transfer request with an explicit Refer-To target. Subscription policy is
    /// still application-owned; observe subsequent NOTIFY requests separately.
    pub fn start_refer(
        &self,
        refer_to: impl Into<crate::sip::ReferTo>,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let mut headers = headers.unwrap_or_default();
        headers.retain(|h| {
            !h.name().eq_ignore_ascii_case("Refer-To") && !h.name().eq_ignore_ascii_case("r")
        });
        headers.push(Header::ReferTo(refer_to.into()));
        self.start_request(Method::Refer, Some(headers), body)
    }
    /// Report transfer progress as a SIP fragment (RFC 3515).
    pub fn start_notify_refer(
        &self,
        status: StatusCode,
        subscription_state: &str,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        let headers = vec![
            Header::Event("refer".into()),
            Header::SubscriptionState(subscription_state.into()),
        ];
        let body = Body::new(
            "message/sipfrag",
            format!("SIP/2.0 {} {:?}\r\n", u16::from(status.clone()), status),
        );
        self.start_request_body(Method::Notify, Some(headers), body)
    }
}

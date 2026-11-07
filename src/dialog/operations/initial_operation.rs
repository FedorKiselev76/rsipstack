//! Stack-owned initial INVITE and explicit, independently observable cancellation.
use super::{
    dialog::DialogState,
    invite_dialog::InviteDialog,
    operation::{DialogOperation, OperationCompletion, OperationError, OperationTrace},
    persistence::DialogCheckpointReason,
};
use crate::{
    platform::{self, sync::Mutex, CancellationToken},
    prelude::*,
    sip::{Header, HeadersExt, Method, Response},
};
use core::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
#[must_use]
/// An initial call attempt owned by the stack. Dropping observers does not
/// cancel it; cancellation has a distinct result from the INVITE.
///
/// ```rust,no_run
/// # async fn example(layer: std::sync::Arc<rsipstack::dialog::dialog_layer::DialogLayer>,
/// # options: rsipstack::dialog::invitation::InviteOption,
/// # states: rsipstack::dialog::dialog::DialogStateSender) -> rsipstack::Result<()> {
/// let (dialog, invite) = layer.start_invite(options, states)?;
/// let cancellation = dialog.start_cancel()?;
/// let cancel_result = cancellation.wait().await;
/// let invite_result = invite.wait().await;
/// // CANCEL's 200 does not establish INVITE's outcome. A late INVITE 200 is
/// // retained separately, including when ACK or cleanup fails.
/// let cancel_response = cancel_result.response();
/// let invite_response = invite_result.response();
/// let signaling = invite_result.signaling_result();
/// # Ok(()) }
/// ```
pub struct InitialInviteOperation {
    control: Arc<InitialControl>,
}
impl InitialInviteOperation {
    pub(crate) fn new(control: Arc<InitialControl>) -> Self {
        Self { control }
    }
    pub fn operation(&self) -> DialogOperation {
        DialogOperation {
            trace: self.control.trace.clone(),
        }
    }
    pub async fn wait(&self) -> OperationCompletion {
        self.operation().wait().await
    }
    /// Accept a cancellation intent synchronously. Repeated calls observe the
    /// same cancellation. A successful CANCEL does not imply INVITE rejection.
    pub fn cancel(&self) -> CancelOperation {
        self.control.cancel()
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CancelCompletion {
    /// No initial INVITE was sent, so no CANCEL transaction was needed.
    SuppressedBeforeSend,
    /// INVITE completed before a CANCEL could be sent. Inspect its own result.
    InviteAlreadyFinal,
    /// The independently owned CANCEL transaction finished.
    Transaction(OperationCompletion),
    Failed(OperationError),
}
impl CancelCompletion {
    pub fn response(&self) -> Option<&Response> {
        match self {
            Self::Transaction(result) => result.response(),
            _ => None,
        }
    }
    pub fn signaling_result(&self) -> core::result::Result<(), &OperationError> {
        match self {
            Self::Transaction(result) => result.signaling_result(),
            Self::Failed(error) => Err(error),
            _ => Ok(()),
        }
    }
}
struct CancelState {
    value: Mutex<Option<CancelCompletion>>,
    done: CancellationToken,
}
#[derive(Clone)]
#[must_use]
pub struct CancelOperation {
    state: Arc<CancelState>,
}
impl CancelOperation {
    pub(crate) fn already_final() -> Self {
        let operation = Self::new();
        operation.complete(CancelCompletion::InviteAlreadyFinal);
        operation
    }
    fn new() -> Self {
        Self {
            state: Arc::new(CancelState {
                value: Mutex::new(None),
                done: CancellationToken::new(),
            }),
        }
    }
    fn complete(&self, value: CancelCompletion) {
        let mut stored = self.state.value.lock();
        if stored.is_none() {
            *stored = Some(value);
            self.state.done.cancel();
        }
    }
    pub async fn wait(&self) -> CancelCompletion {
        self.state.done.cancelled().await;
        self.state
            .value
            .lock()
            .clone()
            .expect("cancellation result precedes notification")
    }
}
struct CancelDriver(CancelOperation);
impl Drop for CancelDriver {
    fn drop(&mut self) {
        self.0
            .complete(CancelCompletion::Failed(OperationError::DriverStopped));
    }
}

pub(crate) struct InitialControl {
    dialog: InviteDialog,
    pub(crate) trace: Arc<OperationTrace>,
    provisional: Mutex<CancellationToken>,
    final_seen: AtomicBool,
    pub(crate) finished: CancellationToken,
    pub(crate) suppressed: AtomicBool,
    released: AtomicBool,
    cancellation: Mutex<Option<CancelOperation>>,
    cancel_headers: Mutex<Vec<Header>>,
    pub(super) cleanup: Mutex<Option<OperationCompletion>>,
}
impl InitialControl {
    pub(crate) fn new(dialog: InviteDialog) -> Arc<Self> {
        Arc::new(Self {
            trace: OperationTrace::admitted(&dialog.inner),
            dialog,
            provisional: Mutex::new(CancellationToken::new()),
            final_seen: AtomicBool::new(false),
            finished: CancellationToken::new(),
            suppressed: AtomicBool::new(false),
            released: AtomicBool::new(false),
            cancellation: Mutex::new(None),
            cancel_headers: Mutex::new(Vec::new()),
            cleanup: Mutex::new(None),
        })
    }
    pub(crate) fn release_operation(&self) {
        if !self.released.swap(true, Ordering::SeqCst) {
            self.dialog.inner.finish_owned_operation();
        }
    }
    pub(super) fn termination_headers(&self) -> Option<Vec<Header>> {
        let headers = self.cancel_headers.lock().clone();
        (!headers.is_empty()).then_some(headers)
    }
    pub(crate) fn begin_attempt(&self) -> bool {
        let _state = self.dialog.inner.state.lock();
        if self.dialog.inner.cancel_requested.load(Ordering::SeqCst) {
            return false;
        }
        let mut ready = self.provisional.lock();
        let previous = core::mem::replace(&mut *ready, CancellationToken::new());
        self.final_seen.store(false, Ordering::SeqCst);
        previous.cancel();
        true
    }
    pub(crate) fn observe_response(&self, response: &Response) {
        let ready = self.provisional.lock();
        if response.status_code.kind() != crate::sip::StatusCodeKind::Provisional {
            self.final_seen.store(true, Ordering::SeqCst);
        }
        ready.cancel();
    }
    pub(crate) fn cancel(self: &Arc<Self>) -> CancelOperation {
        self.cancel_with_headers(Vec::new())
    }
    pub(crate) fn cancel_with_headers(self: &Arc<Self>, headers: Vec<Header>) -> CancelOperation {
        let mut current = self.cancellation.lock();
        if let Some(operation) = &*current {
            return operation.clone();
        }
        let operation = CancelOperation::new();
        *current = Some(operation.clone());
        *self.cancel_headers.lock() = headers;
        {
            let state = self.dialog.inner.state.lock();
            if !matches!(
                *state,
                DialogState::Calling(_) | DialogState::Trying(_) | DialogState::Early(..)
            ) {
                operation.complete(CancelCompletion::InviteAlreadyFinal);
                return operation;
            }
            self.dialog.inner.record_local_termination();
            self.dialog
                .inner
                .cancel_requested
                .store(true, Ordering::SeqCst);
        }
        let control = self.clone();
        let observer = operation.clone();
        let guard = CancelDriver(observer.clone());
        platform::spawn(async move {
            let _guard = guard;
            let outcome = control.drive_cancel().await.unwrap_or_else(|error| {
                CancelCompletion::Failed(OperationError::Protocol(error.to_string()))
            });
            observer.complete(outcome);
        });
        operation
    }
    async fn drive_cancel(&self) -> crate::Result<CancelCompletion> {
        self.dialog
            .inner
            .checkpoint(DialogCheckpointReason::LocalTerminationRequested, None)
            .await?;
        loop {
            let ready = self.provisional.lock().clone();
            platform::select2(
                core::pin::pin!(ready.cancelled()),
                core::pin::pin!(self.finished.cancelled()),
            )
            .await;
            if self.finished.is_cancelled() || self.provisional.lock().is_cancelled() {
                break;
            }
            // Authentication/422 started another transaction. Its CANCEL must
            // wait for that attempt's own provisional response.
        }
        if self.suppressed.load(Ordering::SeqCst) {
            return Ok(CancelCompletion::SuppressedBeforeSend);
        }
        if self.finished.is_cancelled()
            || self.final_seen.load(Ordering::SeqCst)
            || !self.dialog.inner.can_cancel()
        {
            return Ok(CancelCompletion::InviteAlreadyFinal);
        }
        let trace = OperationTrace::admitted(&self.dialog.inner);
        let result = async {
            let mut request = self.dialog.initial_request();
            let seq = request.cseq_header()?.seq()?;
            request.method = Method::Cancel;
            request.body.clear();
            request
                .headers
                .retain(|h| !matches!(h, Header::ContentType(_) | Header::ContentLength(_)));
            request.headers.unique_push(Header::ContentLength(0.into()));
            request.headers.extend(self.cancel_headers.lock().clone());
            request
                .cseq_header_mut()?
                .mut_seq(seq)?
                .mut_method(Method::Cancel)?;
            let mut tx = self.dialog.inner.create_dialog_transaction(request, None)?;
            tx.operation = Some(trace.clone());
            self.dialog
                .inner
                .send_owned_transaction(tx, None, None)
                .await
        }
        .await;
        self.dialog.inner.finish_owned_operation();
        let saved = self
            .dialog
            .inner
            .checkpoint(DialogCheckpointReason::SignalingCompleted, None)
            .await;
        trace.complete(&self.dialog.inner, saved.and(result));
        Ok(CancelCompletion::Transaction(
            DialogOperation { trace }.wait().await,
        ))
    }
}

pub(crate) struct InitialDriverGuard(pub(crate) Arc<InitialControl>);
impl Drop for InitialDriverGuard {
    fn drop(&mut self) {
        if !self.0.finished.is_cancelled() {
            self.0.release_operation();
            self.0.trace.failed(OperationError::DriverStopped);
            self.0.trace.complete(
                &self.0.dialog.inner,
                Err(crate::Error::Error("initial INVITE driver stopped".into())),
            );
            self.0.finished.cancel();
        }
    }
}

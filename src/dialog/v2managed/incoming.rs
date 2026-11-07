use super::*;
/// An owned incoming INVITE exchange, including logical early branches.
#[derive(Clone)]
pub struct IncomingInvite(
    pub(super) crate::dialog::incoming::IncomingInvite,
    pub(super) Arc<control::Control>,
);
impl IncomingInvite {
    pub fn dialog(&self) -> InviteDialog {
        InviteDialog::new(self.0.dialog(), self.1.clone())
    }
    pub fn branch(&self) -> core::result::Result<InviteDialog, ResponseError> {
        self.0
            .branch()
            .map(|d| InviteDialog::new(d, self.1.clone()))
    }
    pub fn cancellation(&self) -> crate::platform::CancellationToken {
        self.0.cancellation()
    }
    pub async fn wait(&self) -> core::result::Result<(), OperationError> {
        self.0.wait().await
    }
}

pub use crate::dialog::incoming::IncomingRequestOperation;

/// An incoming in-dialog request requiring application response policy.
/// Request bytes are owned data; only the response handle can affect its exchange.
#[derive(Clone, Debug)]
pub struct IncomingRequest {
    id: DialogId,
    request: Request,
    response: TransactionHandle,
}
impl IncomingRequest {
    pub(super) fn new(id: DialogId, request: Request, response: TransactionHandle) -> Self {
        Self {
            id,
            request,
            response,
        }
    }
    pub fn id(&self) -> &DialogId {
        &self.id
    }
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn responder(&self) -> &TransactionHandle {
        &self.response
    }
    pub fn into_parts(self) -> (DialogId, Request, TransactionHandle) {
        (self.id, self.request, self.response)
    }
    pub fn start_response(
        &self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        self.response.start_response(status, headers, body)
    }
}

impl IncomingRequest {
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

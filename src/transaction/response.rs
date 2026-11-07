//! Owned final responses for requests handled without a dialog.
//!
//! This uses the same receipts as dialog responses. It is intended for local
//! responses such as standalone OPTIONS, admission rejection or an unmatched
//! in-dialog request. A successful INVITE belongs to the dialog layer; proxy
//! responses belong to the proxy layer. No dialog is created by this API.
pub use crate::dialog::response_operation::{ResponseError, ResponseProgress, ResponseReceipt};
use crate::{
    dialog::response_operation::ResponseSubmission,
    platform,
    prelude::*,
    sip::{Header, HeadersExt, Method, StatusCode},
    transaction::{make_tag, Transaction, TransactionState, TransactionType},
};

impl Transaction {
    /// Admit one local final response and give the stack ownership of sending
    /// it and retaining the server transaction for its protocol lifetime.
    /// Dropping the receipt or a waiter does not cancel accepted work.
    ///
    /// `wait_sent()` observes transport acceptance, not peer receipt or full
    /// transaction retirement. For a rejected INVITE, `wait_ack()` separately
    /// observes its ACK or timeout. Non-INVITE responses have no ACK. Endpoint
    /// shutdown interrupts unfinished work with `ResponseError::DriverStopped`.
    ///
    /// Only server transactions and final statuses (200..699) are accepted.
    /// ACK never receives a response. Successful INVITE responses require the
    /// dialog API. Identity, routing and Content-Length cannot be overridden
    /// through extension headers. Invalid admission consumes the transaction
    /// without sending a fallback response. Existing raw methods remain usable
    /// before handing ownership to this method.
    ///
    /// ```no_run
    /// # use rsipstack::{sip::StatusCode, transaction::Transaction};
    /// # async fn availability(tx: Transaction) -> Result<(), Box<dyn std::error::Error>> {
    /// let receipt = tx.start_final_response(StatusCode::OK, None, None)?;
    /// receipt.wait_sent().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Ownership prevents sending another final response through the same
    /// transaction object:
    ///
    /// ```compile_fail
    /// # use rsipstack::{sip::StatusCode, transaction::Transaction};
    /// # fn twice(tx: Transaction) {
    /// let first = tx.start_final_response(StatusCode::OK, None, None);
    /// let second = tx.start_final_response(StatusCode::BadRequest, None, None);
    /// # }
    /// ```
    pub fn start_final_response(
        mut self,
        status: StatusCode,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<ResponseReceipt, ResponseError> {
        let reject = |message: &str| ResponseError::Protocol(message.into());
        if !matches!(
            self.transaction_type,
            TransactionType::ServerInvite | TransactionType::ServerNonInvite
        ) || self.original.method == Method::Ack
        {
            return Err(reject(
                "a response requires a server transaction other than ACK",
            ));
        }
        if !(200..=699).contains(&status.code()) {
            return Err(reject("a final response status must be 200..699"));
        }
        if self.original.method == Method::Invite && status.code() < 300 {
            return Err(reject("use the dialog API to accept an INVITE"));
        }
        if matches!(
            self.state,
            TransactionState::Accepted
                | TransactionState::Completed
                | TransactionState::Confirmed
                | TransactionState::Terminated
        ) {
            return Err(ResponseError::FinalAlreadyAdmitted);
        }
        if headers.iter().flatten().any(|header| {
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
                "route",
                "record-route",
                "content-length",
                "l",
            ]
            .iter()
            .any(|name| header.name().trim().eq_ignore_ascii_case(name))
        }) {
            return Err(reject(
                "response identity, routing and length are stack-owned",
            ));
        }
        let mut response = self
            .endpoint_inner
            .make_response(&self.original, status, body);
        let to = response
            .to_header_mut()
            .map_err(|e| reject(&e.to_string()))?;
        if to.tag().map_err(|e| reject(&e.to_string()))?.is_none() {
            let tag = self
                .last_response
                .as_ref()
                .and_then(|r| r.to_header().ok()?.tag().ok().flatten())
                .unwrap_or_else(make_tag);
            to.mut_tag(tag).map_err(|e| reject(&e.to_string()))?;
        }
        response.headers.extend(headers.unwrap_or_default());
        let (receipt, producer) = ResponseReceipt::pair();
        producer.trace().bind(&self.original, &response);
        let submission = ResponseSubmission {
            response,
            producer: Some(producer.clone()),
            initial_owner: None,
        };
        let stop = self.endpoint_inner.shutdown_token();
        platform::spawn(async move {
            // Retain a producer even if the driver is interrupted during send.
            let _producer = producer;
            if stop.is_cancelled() {
                return;
            }
            let driver = async move {
                if self.respond_submission(submission).await.is_ok() {
                    while self.receive().await.is_some() {}
                }
            };
            // Forced endpoint shutdown is the only external cancellation
            // boundary; observer cancellation never interrupts receive/send.
            let _ =
                platform::select2(core::pin::pin!(driver), core::pin::pin!(stop.cancelled())).await;
        });
        Ok(receipt)
    }
}

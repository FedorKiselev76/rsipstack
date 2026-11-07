use crate::dialog::dialog::{Dialog, TransactionCommand, TransactionHandle};
use crate::{platform::mpsc, prelude::*, sip::StatusCode, transaction::Transaction, Result};

impl TransactionHandle {
    #[deprecated(note = "Use start_response and wait_sent to observe response transmission")]
    pub async fn reply(
        &self,
        status: StatusCode,
    ) -> core::result::Result<(), mpsc::error::SendError<TransactionCommand>> {
        self.respond(status, None, None).await
    }

    #[deprecated(note = "Use start_response and wait_sent to observe response transmission")]
    pub async fn respond(
        &self,
        status: StatusCode,
        headers: Option<Vec<crate::sip::Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<(), mpsc::error::SendError<TransactionCommand>> {
        self.sender
            .send(TransactionCommand::Respond {
                status,
                headers,
                body,
            })
            .await
    }
}

impl Dialog {
    #[deprecated(
        note = "Use start_incoming_request to admit requests before asynchronous dispatch"
    )]
    pub async fn handle(&mut self, tx: &mut Transaction) -> Result<()> {
        match self {
            Dialog::Invite(d) => d.handle(tx).await,
            Dialog::ServerSubscription(d) => d.handle(tx).await,
            Dialog::ClientSubscription(d) => d.handle(tx).await,
            Dialog::ServerPublication(d) => d.handle(tx).await,
            Dialog::ClientPublication(d) => d.handle(tx).await,
        }
    }

    pub async fn hangup(&self) -> Result<()> {
        self.hangup_with_headers(None).await
    }

    pub async fn hangup_with_headers(
        &self,
        headers: Option<Vec<crate::sip::Header>>,
    ) -> Result<()> {
        match self {
            Dialog::Invite(d) => d.hangup_with_headers(headers).await,
            Dialog::ServerSubscription(d) => d.unsubscribe_with_headers(headers).await,
            Dialog::ClientSubscription(d) => d.unsubscribe_with_headers(headers).await,
            Dialog::ServerPublication(d) => d.close_with_headers(headers).await,
            Dialog::ClientPublication(d) => d.close_with_headers(headers).await,
        }
    }

    pub async fn request(
        &self,
        method: crate::sip::Method,
        headers: Option<Vec<crate::sip::Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<crate::sip::Response>> {
        match self {
            Dialog::Invite(d) => d.request(method, headers, body).await,
            Dialog::ServerSubscription(d) => d.request(method, headers, body).await,
            Dialog::ClientSubscription(d) => d.request(method, headers, body).await,
            Dialog::ServerPublication(d) => d.request(method, headers, body).await,
            Dialog::ClientPublication(d) => d.request(method, headers, body).await,
        }
    }

    pub async fn refer(
        &self,
        refer_to: impl Into<crate::sip::ReferTo>,
        headers: Option<Vec<crate::sip::Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<crate::sip::Response>> {
        match self {
            Dialog::Invite(d) => d.refer(refer_to, headers, body).await,
            Dialog::ServerSubscription(d) => d.refer(refer_to, headers, body).await,
            Dialog::ClientSubscription(d) => d.refer(refer_to, headers, body).await,
            Dialog::ServerPublication(d) => d.refer(refer_to, headers, body).await,
            Dialog::ClientPublication(d) => d.refer(refer_to, headers, body).await,
        }
    }

    pub async fn message(
        &self,
        headers: Option<Vec<crate::sip::Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Option<crate::sip::Response>> {
        match self {
            Dialog::Invite(d) => d.message(headers, body).await,
            Dialog::ServerSubscription(d) => d.message(headers, body).await,
            Dialog::ClientSubscription(d) => d.message(headers, body).await,
            Dialog::ServerPublication(d) => d.message(headers, body).await,
            Dialog::ClientPublication(d) => d.message(headers, body).await,
        }
    }
}

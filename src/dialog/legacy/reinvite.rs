use crate::dialog::reinvite::*;
use crate::dialog::{invite_dialog::InviteDialog, DialogId};
use crate::{
    prelude::*,
    sip::{Header, HeadersExt, Method, Request, Response},
    transaction::key::TransactionRole,
    Result,
};
impl InviteDialog {
    /// Build a re-INVITE and reserve its CSeq without sending it. The caller can
    /// inspect the request and persist a snapshot before sending. Authentication
    /// retries, when configured, can allocate additional CSeq values. Send each
    /// prepared request once, before preparing another in-dialog request.
    #[deprecated(
        note = "Use start_reinvite; the stack reserves CSeq and supplies checkpoints before sending"
    )]
    pub fn prepare_reinvite(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> Result<Request> {
        if !self.inner.is_confirmed() {
            return Err(crate::Error::Error(
                "re-INVITE requires a confirmed dialog".into(),
            ));
        }
        self.inner
            .make_request(Method::Invite, None, None, None, headers, body)
    }

    /// Send a prepared re-INVITE. Offerless requests require an answer handler;
    /// requests carrying an offer must not supply one. The handler owns SDP
    /// policy, while the stack owns ACK routing, caching and retransmission.
    /// Once polled, the operation runs in an owning task: dropping this awaiter
    /// does not cancel SIP processing. Use the cancellation token for CANCEL.
    #[deprecated(note = "Use start_reinvite and observe its completion receipt")]
    pub async fn send_reinvite(
        &self,
        request: Request,
        options: ReinviteOptions,
    ) -> Result<Option<Response>> {
        if !self.inner.is_confirmed()
            || request.method != Method::Invite
            || request.cseq_header()?.method()? != Method::Invite
            || request.cseq_header()?.seq()? != self.inner.get_local_seq()
            || DialogId::try_from((&request, TransactionRole::Client))? != self.id()
            || request.body.is_empty() != options.answer_handler.is_some()
        {
            return Err(crate::Error::Error(
                "invalid prepared re-INVITE or ACK answer handler".into(),
            ));
        }
        let inner = self.inner.clone();
        let task =
            crate::platform::spawn_with_result(
                async move { inner.do_reinvite(request, options).await },
            );
        #[cfg(feature = "platform-tokio")]
        {
            task.await
                .map_err(|error| crate::Error::Error(error.to_string()))?
        }
        #[cfg(not(feature = "platform-tokio"))]
        {
            task.await
        }
    }
}

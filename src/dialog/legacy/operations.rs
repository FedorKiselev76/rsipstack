//! Compatibility entry points for the first operation API experiment.
//! Raw legacy state can be externally mutated; it cannot promise managed admission.
use crate::dialog::{
    invite_dialog::InviteDialog,
    operation::{AdmissionError, DialogOperation, OperationCompletion},
    reinvite::ReinviteOptions,
    termination::{TerminationOperation, TerminationOptions},
};
use crate::{
    prelude::*,
    sip::{Header, Method},
};
impl InviteDialog {
    fn require_managed_state(&self) -> core::result::Result<(), AdmissionError> {
        if self.inner.owned_state {
            Ok(())
        } else {
            Err(AdmissionError::ExternallyOwnedState)
        }
    }
    pub fn start_request(
        &self,
        method: Method,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        self.require_managed_state()?;
        self.admit_request(method, headers, body)
    }
    pub fn start_reinvite(
        &self,
        headers: Option<Vec<Header>>,
        body: Option<Vec<u8>>,
        options: ReinviteOptions,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        self.require_managed_state()?;
        self.admit_reinvite(headers, body, options)
    }
    pub fn start_late_answer_cleanup(
        &self,
        result: &OperationCompletion,
        headers: Option<Vec<Header>>,
    ) -> core::result::Result<DialogOperation, AdmissionError> {
        self.require_managed_state()?;
        self.admit_late_answer_cleanup(result, headers)
    }
    pub fn start_termination(
        &self,
        options: TerminationOptions,
    ) -> core::result::Result<TerminationOperation, AdmissionError> {
        self.require_managed_state()?;
        self.admit_termination(options)
    }
}

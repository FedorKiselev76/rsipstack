use crate::dialog::{
    dialog::{Dialog, DialogState, DialogStateSender, TerminatedReason},
    dialog_layer::{DialogLayer, DialogLayerInnerRef},
    invitation::{bye_if_2xx, InviteAsyncResult, InviteOption},
    invite_dialog::InviteDialog,
    DialogId,
};
use crate::{
    prelude::*,
    sip::{Response, SipMessage, StatusCodeKind},
    transaction::Transaction,
    Result,
};
use futures::FutureExt;
use tracing::{debug, info, warn};

pub struct DialogGuard {
    pub dialog_layer_inner: DialogLayerInnerRef,
    pub id: DialogId,
}

impl DialogGuard {
    pub fn new(dialog_layer: &Arc<DialogLayer>, id: DialogId) -> Self {
        Self {
            dialog_layer_inner: dialog_layer.inner.clone(),
            id,
        }
    }
}

impl Drop for DialogGuard {
    fn drop(&mut self) {
        let dlg = match self.dialog_layer_inner.dialogs.remove(&self.id) {
            Some(dlg) => dlg,
            None => return,
        };
        let _handle = crate::platform::spawn(async move {
            if let Err(e) = dlg.hangup().await {
                info!(id = %dlg.id(), error = %e, "failed to hangup dialog");
            }
        });
    }
}

pub(super) struct DialogGuardForUnconfirmed {
    pub dialog_layer_inner: DialogLayerInnerRef,
    pub id: DialogId,
    invite_tx: Option<Transaction>,
    /// Whether the owning future started processing the INVITE.
    started: bool,
    /// The INVITE's dialog, `None` once `process_invite` returned.
    dialog: Option<InviteDialog>,
}

impl Drop for DialogGuardForUnconfirmed {
    fn drop(&mut self) {
        // Normal completion has handed ownership to the caller, even when
        // the confirmed key equals the early key (a tag-less 2xx).
        if self.dialog.is_none() {
            return;
        }
        let client_dialog = match self.dialog_layer_inner.dialogs.remove(&self.id) {
            Some(Dialog::Invite(client_dialog)) => client_dialog,
            Some(_) => return,
            // The application already removed the dialog from the layer
            // (`DialogLayer::remove_dialog`): its INVITE still has to end.
            None => match self.dialog.take() {
                Some(client_dialog) => client_dialog,
                // `process_invite` returned: `do_invite` handles the outcome.
                None => return,
            },
        };

        // A task dropped before its first poll never sent an INVITE.
        if !self.started {
            client_dialog.inner.transition(DialogState::Terminated(
                client_dialog.id(),
                TerminatedReason::UacCancel,
            ));
            return;
        }

        match client_dialog.state() {
            DialogState::Terminated(_, _) => {}
            state @ (DialogState::Calling(_)
            | DialogState::Trying(_)
            | DialogState::Early(_, _)) => {
                // CANCEL cannot be sent before a provisional response (RFC 3261
                // §9.1): a Calling dialog ends now, its INVITE is CANCELed once
                // a provisional arrives.
                let calling = matches!(state, DialogState::Calling(_));
                let invite_tx = self.invite_tx.take();
                if calling || invite_tx.is_none() {
                    client_dialog.inner.transition(DialogState::Terminated(
                        client_dialog.id(),
                        TerminatedReason::UacCancel,
                    ));
                }
                let Some(mut invite_tx) = invite_tx else {
                    return;
                };

                debug!(%self.id, "unconfirmed dialog dropped, cancelling it");
                let _handle = crate::platform::spawn(async move {
                    if calling {
                        invite_tx.stop_retransmissions();
                        let window = invite_tx.endpoint_inner.option.t1x64;
                        let first = wait_response(&mut invite_tx, window, false).await;
                        if !first
                            .as_ref()
                            .is_some_and(|r| r.status_code.kind() == StatusCodeKind::Provisional)
                        {
                            drop(invite_tx);
                            bye_if_2xx(&client_dialog, first).await;
                            return;
                        }
                    }
                    let final_window = invite_tx.endpoint_inner.option.t1x64;
                    invite_tx.stop_retransmissions();

                    // CANCEL is a separate transaction. Keep driving it to its
                    // own response or timeout even if the INVITE finishes first.
                    let cancel_dialog = client_dialog.clone();
                    let _handle = crate::platform::spawn(async move {
                        if let Err(e) = cancel_dialog.send_cancel().await {
                            warn!(id = %cancel_dialog.id(), error = %e, "dialog cancel failed");
                        }
                    });
                    if !calling {
                        client_dialog.inner.transition(DialogState::Terminated(
                            client_dialog.id(),
                            TerminatedReason::UacCancel,
                        ));
                        debug!(id = %client_dialog.id(), "dialog terminated");
                    }

                    // The callee may still answer after the CANCEL: keep the
                    // INVITE transaction until its final response (up to
                    // 64*T1) so a 2xx is ACKed, then end that session with a
                    // BYE (RFC 3261 §9.1, §15).
                    let final_response = wait_response(&mut invite_tx, final_window, true).await;
                    // Closing the command channel terminates the dialog loop.
                    drop(invite_tx);
                    bye_if_2xx(&client_dialog, final_response).await;
                });
            }
            // A completed invite task is NOT an abandoned invite: for
            // `do_invite_async` the background task always ends right after
            // the dialog confirms (the JoinHandle detaches), and the
            // application owns the confirmed dialog from `Confirmed` on.
            // Hanging it up here killed established calls whenever the
            // dialog's registry entry was still reachable under the early
            // (tag-less) key — e.g. a 200 OK without a To tag, whose
            // confirmed key equals the early key. Unconfirmed branches above
            // keep their CANCEL cleanup (those ARE abandonment).
            DialogState::Confirmed(_, _) => {}
            _ => {}
        }
    }
}

/// Wait up to `limit` for the INVITE transaction's first response, or its
/// first final one.
async fn wait_response(
    invite_tx: &mut Transaction,
    limit: core::time::Duration,
    final_only: bool,
) -> Option<Response> {
    let wait = async {
        while let Some(msg) = invite_tx.receive().await {
            if let SipMessage::Response(resp) = msg {
                if !final_only || resp.status_code.kind() != StatusCodeKind::Provisional {
                    return Some(resp);
                }
            }
        }
        None
    };
    crate::platform::timeout(limit, wait).await.ok().flatten()
}

/// End the session a 2xx to an abandoned INVITE established.

impl DialogLayer {
    /// Send an INVITE request and create a client dialog
    ///
    /// This is the main method for initiating outbound calls. It creates
    /// an INVITE request, sends it, and manages the resulting dialog.
    /// The method handles the complete INVITE transaction including
    /// authentication challenges and response processing.
    ///
    /// # Parameters
    ///
    /// * `opt` - INVITE options containing all call parameters
    /// * `state_sender` - Channel for receiving dialog state updates
    ///
    /// # Returns
    ///
    /// * `Ok((InviteDialog, Option<Response>))` - Created dialog and final response
    /// * `Err(Error)` - Failed to send INVITE or process responses
    ///
    /// # Call Flow
    ///
    /// 1. Creates INVITE request from options
    /// 2. Creates client dialog and transaction
    /// 3. Sends INVITE request
    /// 4. Processes responses (1xx, 2xx, 3xx-6xx)
    /// 5. Handles authentication challenges if needed
    /// 6. Returns established dialog and final response
    ///
    /// # Examples
    ///
    /// ## Basic Call Setup
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # async fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// # let state_sender = todo!();
    /// let (dialog, response) = dialog_layer.do_invite(invite_option, state_sender).await?;
    ///
    /// if let Some(resp) = response {
    ///     match resp.status_code {
    ///         rsipstack::sip::StatusCode::OK => {
    ///             println!("Call answered!");
    ///             // Process SDP answer in resp.body
    ///         },
    ///         rsipstack::sip::StatusCode::BusyHere => {
    ///             println!("Called party is busy");
    ///         },
    ///         _ => {
    ///             println!("Call failed: {}", resp.status_code);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Monitoring Dialog State
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # use rsipstack::dialog::dialog::DialogState;
    /// # async fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// let (state_tx, mut state_rx) = tokio::sync::mpsc::unbounded_channel();
    /// let (dialog, response) = dialog_layer.do_invite(invite_option, state_tx).await?;
    ///
    /// // Monitor dialog state changes
    /// tokio::spawn(async move {
    ///     while let Some(state) = state_rx.recv().await {
    ///         match state {
    ///             DialogState::Early(_, resp) => {
    ///                 println!("Ringing: {}", resp.status_code);
    ///             },
    ///             DialogState::Confirmed(_,_) => {
    ///                 println!("Call established");
    ///             },
    ///             DialogState::Terminated(_, code) => {
    ///                 println!("Call ended: {:?}", code);
    ///                 break;
    ///             },
    ///             _ => {}
    ///         }
    ///     }
    /// });
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Error Handling
    ///
    /// The method can fail for various reasons:
    /// * Network connectivity issues
    /// * Authentication failures
    /// * Invalid SIP URIs or headers
    /// * Transaction timeouts
    /// * Protocol violations
    ///
    /// # Important: Cleanup Required
    ///
    /// When a 2xx response is received, the confirmed dialog is registered in
    /// the internal dialog registry (not when [`InviteDialog::cancel`] was
    /// called: that 2xx is ended with a BYE). **The caller is responsible for removing it**
    /// after the dialog terminates by calling [`DialogLayer::remove_dialog`]
    /// with the dialog's [`DialogId`].
    ///
    /// The recommended pattern is to listen for
    /// [`DialogState::Terminated`](crate::dialog::dialog::DialogState::Terminated)
    /// on the `state_sender` channel and call `remove_dialog` when received.
    /// See the "Monitoring Dialog State" example above.
    ///
    /// Failure to call `remove_dialog` will cause the dialog to remain in the
    /// registry indefinitely, resulting in a memory leak.
    ///
    /// # Authentication
    ///
    /// If credentials are provided in the options, the method will
    /// automatically handle 401/407 authentication challenges by
    /// resending the request with proper authentication headers.
    #[deprecated(
        note = "Use start_invite and its completion receipt; dropping a waiter must not abort signaling"
    )]
    pub async fn do_invite(
        &self,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(InviteDialog, Option<Response>)> {
        let (dialog, tx) = self.create_client_invite_dialog(opt, state_sender)?;
        let id = dialog.id();

        self.inner
            .dialogs
            .insert(id.clone(), Dialog::Invite(dialog.clone()));

        let mut guard = DialogGuardForUnconfirmed {
            dialog_layer_inner: self.inner.clone(),
            id: id.clone(),
            invite_tx: Some(tx),
            started: true,
            dialog: Some(dialog.clone()),
        };

        let tx = guard
            .invite_tx
            .as_mut()
            .expect("transcation should be avaible");

        let r = dialog
            .process_invite_with_registration(tx, |new_id| {
                self.inner.dialogs.with_mut(|dialogs| {
                    dialogs.remove(&id);
                    dialogs.insert(new_id.clone(), Dialog::Invite(dialog.clone()));
                });
            })
            .boxed()
            .await;
        guard.dialog = None;
        if !dialog.inner.is_confirmed() {
            self.inner.dialogs.remove(&id);
        }

        match r {
            Ok((new_dialog_id, resp)) => {
                match resp {
                    Some(ref r)
                        if r.status_code.kind() == crate::sip::StatusCodeKind::Successful =>
                    {
                        // Registration happened before ACK, and must not be
                        // repeated after a concurrent BYE/remove_dialog.
                        // RFC 6026 §7.2: the client transaction is in Accepted
                        // with Timer M (64*T1) armed. Keep receiving until it
                        // ends so retransmitted 2xx keep being re-ACKed and
                        // forked 2xx are observed; abandoning the transaction
                        // here would leave it (and its timers) in the
                        // endpoint's table and silently stop the re-ACKs.
                        if let Some(mut tx) = guard.invite_tx.take() {
                            let dlg = dialog.clone();
                            let confirmed_tag = new_dialog_id.remote_tag.clone();
                            crate::platform::spawn(async move {
                                let mut seen_forks: Vec<String> = Vec::new();
                                while let Some(msg) = tx.receive().await {
                                    dlg.end_forked_branch(&msg, &confirmed_tag, &mut seen_forks)
                                        .await;
                                }
                                debug!(id = %new_dialog_id, "accepted transaction drained (Timer M expired)");
                            });
                        }
                    }
                    _ => {}
                }
                Ok((dialog, resp))
            }
            Err(e) => Err(e),
        }
    }
    /// Asynchronously executes an INVITE transaction in the background.
    ///
    /// Registers the dialog under an early dialog ID while the INVITE is in progress.
    /// On a 2xx response, the early entry is replaced by the confirmed dialog
    /// ID before sending ACK, so incoming requests can be handled immediately.
    /// Returns a JoinHandle resolving to the final dialog ID and response.
    ///
    /// # Important: Cleanup Required
    ///
    /// On a successful (2xx) response the confirmed dialog is registered in
    /// the internal dialog registry (not when [`InviteDialog::cancel`] was
    /// called: that 2xx is ended with a BYE). **The caller must remove it** after the
    /// dialog terminates by calling [`DialogLayer::remove_dialog`] with the
    /// confirmed dialog's [`DialogId`].
    ///
    /// Listen for
    /// [`DialogState::Terminated`](crate::dialog::dialog::DialogState::Terminated)
    /// on the `state_sender` channel and call `remove_dialog` when received.
    /// Failure to do so will keep the dialog in the registry and cause a
    /// memory leak.
    #[deprecated(note = "Use start_invite to observe INVITE, ACK and cancellation outcomes")]
    pub fn do_invite_async(
        self: &Arc<Self>,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(InviteDialog, crate::platform::JoinHandle<InviteAsyncResult>)> {
        let (dialog, task) = self.spawn_legacy_invite(opt, state_sender)?;
        Ok((dialog, task))
    }
    fn spawn_legacy_invite(
        self: &Arc<Self>,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(InviteDialog, crate::platform::JoinHandle<InviteAsyncResult>)> {
        let (dialog, tx) = self.create_client_invite_dialog(opt, state_sender)?;
        let id0 = dialog.id();

        // 1) register early key (so in-dialog requests can be matched)
        self.inner
            .dialogs
            .insert(id0.clone(), Dialog::Invite(dialog.clone()));

        let inner = self.inner.clone();
        let dialog_clone = dialog.clone();

        // Own the guard before spawning so an abort before the first poll
        // also removes the early registry entry.
        let mut guard = DialogGuardForUnconfirmed {
            dialog_layer_inner: inner.clone(),
            id: id0.clone(),
            invite_tx: Some(tx),
            started: false,
            dialog: Some(dialog_clone.clone()),
        };

        // 2) run invite in background, keep registry updated like do_invite()
        let handle = crate::platform::spawn_with_result(async move {
            guard.started = true;
            let invite_tx = guard
                .invite_tx
                .as_mut()
                .expect("transaction should be available");
            let r = dialog_clone
                .process_invite_with_registration(invite_tx, |new_id| {
                    inner.dialogs.with_mut(|dialogs| {
                        dialogs.remove(&id0);
                        dialogs.insert(new_id.clone(), Dialog::Invite(dialog_clone.clone()));
                    });
                })
                .boxed()
                .await;
            guard.dialog = None;
            let mut tx = guard
                .invite_tx
                .take()
                .expect("transaction should be available");

            if !dialog_clone.inner.is_confirmed() {
                inner.dialogs.remove(&id0);
            }

            match &r {
                Ok((new_id, resp_opt)) => {
                    let is_2xx = resp_opt
                        .as_ref()
                        .map(|resp| {
                            resp.status_code.kind() == crate::sip::StatusCodeKind::Successful
                        })
                        .unwrap_or(false);

                    if is_2xx {
                        // The full key was registered before ACK. Do not
                        // resurrect a dialog the application already removed.
                        // RFC 6026 §7.2: keep draining the Accepted transaction
                        // until Timer M ends it (re-ACKs retransmitted 2xx,
                        // observes forked 2xx, and detaches it from the
                        // endpoint's table). See do_invite for the rationale.
                        let confirmed_id = new_id.clone();
                        let confirmed_tag = if is_2xx {
                            new_id.remote_tag.clone()
                        } else {
                            String::new()
                        };
                        let forked_dlg = dialog_clone.clone();
                        crate::platform::spawn(async move {
                            let mut seen_forks: Vec<String> = Vec::new();
                            while let Some(msg) = tx.receive().await {
                                forked_dlg
                                    .end_forked_branch(&msg, &confirmed_tag, &mut seen_forks)
                                    .await;
                            }
                            debug!(id = %confirmed_id, "accepted transaction drained (Timer M expired)");
                        });
                    } else {
                        // Release an unsent INVITE reservation before recording
                        // completion, including cancellation before first poll.
                        drop(tx);
                    }
                }
                Err(e) => {
                    debug!(%id0, error = %e, "async invite failed");
                    drop(tx);
                }
            }

            r
        });

        Ok((dialog, handle))
    }
}

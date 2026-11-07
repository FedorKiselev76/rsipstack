//! Ordered dialog delivery: protocol notifications and incoming requests are
//! different types in the same queue. Splitting them into independent receivers
//! would lose their relative delivery order.
use super::*;
use crate::platform::mpsc;
use crate::sip::Response;

/// Protocol progress. Repeated Confirmed notifications can follow mid-dialog
/// exchanges; use opt-in lifecycle facts when initial acceptance must occur once.
#[derive(Clone, Debug)]
pub enum DialogNotification {
    Calling(DialogId),
    Trying(DialogId),
    Early(DialogId, Response),
    WaitAck(DialogId, Response),
    Confirmed(DialogId, Response),
    Terminated(DialogId, TerminatedReason),
}
impl DialogNotification {
    pub fn id(&self) -> &DialogId {
        match self {
            Self::Calling(id)
            | Self::Trying(id)
            | Self::Early(id, _)
            | Self::WaitAck(id, _)
            | Self::Confirmed(id, _)
            | Self::Terminated(id, _) => id,
        }
    }
}
/// One ordered notification or application-answerable incoming request.
#[derive(Clone, Debug)]
pub enum DialogEvent {
    Notification(DialogNotification),
    IncomingRequest(IncomingRequest),
}
impl DialogEvent {
    pub fn id(&self) -> &DialogId {
        match self {
            Self::Notification(n) => n.id(),
            Self::IncomingRequest(r) => r.id(),
        }
    }
    pub(super) fn from_core(event: crate::dialog::dialog::DialogState) -> Self {
        use crate::dialog::dialog::DialogState as Core;
        let notification = match event {
            Core::Calling(id) => DialogNotification::Calling(id),
            Core::Trying(id) => DialogNotification::Trying(id),
            Core::Early(id, response) => DialogNotification::Early(id, response),
            Core::WaitAck(id, response) => DialogNotification::WaitAck(id, response),
            Core::Confirmed(id, response) => DialogNotification::Confirmed(id, response),
            Core::Terminated(id, reason) => DialogNotification::Terminated(id, reason),
            Core::Updated(id, request, handle)
            | Core::Publish(id, request, handle)
            | Core::Notify(id, request, handle)
            | Core::Refer(id, request, handle)
            | Core::Message(id, request, handle)
            | Core::Info(id, request, handle)
            | Core::Options(id, request, handle) => {
                return Self::IncomingRequest(IncomingRequest::new(
                    id,
                    request,
                    TransactionHandle(handle),
                ))
            }
        };
        Self::Notification(notification)
    }
}
/// Opaque subscription token. Only the stack produces protocol events.
#[derive(Clone)]
pub struct DialogEventSender(pub(super) crate::dialog::dialog::DialogStateSender);
pub struct DialogEventReceiver(crate::dialog::dialog::DialogStateReceiver);
impl DialogEventReceiver {
    pub async fn recv(&mut self) -> Option<DialogEvent> {
        self.0.recv().await.map(DialogEvent::from_core)
    }
}
/// No forwarding task, extra queue, or event reordering is introduced.
pub fn dialog_event_channel() -> (DialogEventSender, DialogEventReceiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (DialogEventSender(sender), DialogEventReceiver(receiver))
}

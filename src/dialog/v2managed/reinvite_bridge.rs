use super::*;
/// Coordinates re-INVITEs while both legs retain their managed ownership.
#[derive(Clone)]
pub struct ReinviteBridge(crate::dialog::reinvite_bridge::ReinviteBridge);
impl ReinviteBridge {
    pub fn new(a: InviteDialog, b: InviteDialog) -> crate::Result<Self> {
        crate::dialog::reinvite_bridge::ReinviteBridge::new(a.0, b.0).map(Self)
    }
    pub fn with_policy(
        a: InviteDialog,
        b: InviteDialog,
        policy: Arc<dyn ReinviteBridgePolicy>,
    ) -> crate::Result<Self> {
        crate::dialog::reinvite_bridge::ReinviteBridge::with_policy(a.0, b.0, policy).map(Self)
    }
    pub fn shutdown(&self) {
        self.0.shutdown();
    }
    pub async fn handle(
        &self,
        source: DialogId,
        request: Request,
        handle: TransactionHandle,
    ) -> crate::Result<StatusCode> {
        self.0.handle(source, request, handle.0).await
    }
}

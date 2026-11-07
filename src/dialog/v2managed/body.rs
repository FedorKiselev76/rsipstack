//! Owned message bodies with an explicit content type. Constructing from a Vec
//! moves its allocation; it does not copy the payload.
use super::*;
use alloc::borrow::Cow;

#[derive(Clone, Debug)]
pub struct Body {
    content_type: Cow<'static, str>,
    bytes: Vec<u8>,
}
impl Body {
    pub fn sdp(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            content_type: Cow::Borrowed("application/sdp"),
            bytes: bytes.into(),
        }
    }
    pub fn text(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            content_type: Cow::Borrowed("text/plain"),
            bytes: bytes.into(),
        }
    }
    pub fn new(content_type: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            content_type: Cow::Owned(content_type.into()),
            bytes: bytes.into(),
        }
    }
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
    pub(super) fn apply(
        self,
        headers: Option<Vec<Header>>,
    ) -> (Option<Vec<Header>>, Option<Vec<u8>>) {
        let mut headers = headers.unwrap_or_default();
        headers.retain(|h| {
            !h.name().eq_ignore_ascii_case("Content-Type") && !h.name().eq_ignore_ascii_case("c")
        });
        headers.push(Header::ContentType(self.content_type.into_owned().into()));
        (Some(headers), Some(self.bytes))
    }
}

/// Convenience builder over the shared, owned initial INVITE options.
/// Conversion concerns configuration data only, never a live legacy handle.
#[derive(Clone, Default)]
pub struct InviteOptions(InviteOption);
impl InviteOptions {
    pub fn new(caller: crate::sip::Uri, callee: crate::sip::Uri, contact: crate::sip::Uri) -> Self {
        Self(InviteOption {
            caller,
            callee,
            contact,
            ..Default::default()
        })
    }
    pub fn body(mut self, body: Body) -> Self {
        self.0.content_type = Some(body.content_type.into_owned());
        self.0.offer = Some(body.bytes);
        if let Some(headers) = &mut self.0.headers {
            headers.retain(|h| {
                !h.name().eq_ignore_ascii_case("Content-Type")
                    && !h.name().eq_ignore_ascii_case("c")
            });
        }
        self
    }
    pub fn headers(mut self, mut headers: Vec<Header>) -> Self {
        if self.0.content_type.is_some() {
            headers.retain(|h| {
                !h.name().eq_ignore_ascii_case("Content-Type")
                    && !h.name().eq_ignore_ascii_case("c")
            });
        }
        self.0.headers = Some(headers);
        self
    }
    pub fn credential(mut self, credential: Credential) -> Self {
        self.0.credential = Some(credential);
        self
    }
    pub fn destination(mut self, destination: crate::transport::SipAddr) -> Self {
        self.0.destination = Some(destination);
        self
    }
    pub fn request_uri(mut self, uri: crate::sip::Uri) -> Self {
        self.0.request_uri = Some(uri);
        self
    }
    pub fn call_id(mut self, call_id: String) -> Self {
        self.0.call_id = Some(call_id);
        self
    }
    pub fn into_options(self) -> InviteOption {
        self.0
    }
}
impl From<InviteOption> for InviteOptions {
    fn from(options: InviteOption) -> Self {
        Self(options)
    }
}
impl From<InviteOptions> for InviteOption {
    fn from(options: InviteOptions) -> Self {
        options.0
    }
}

use super::*;
use crate::sip::{typed::Route, uri::ParamsExt, Param, Scheme, ToTypedHeader, Transport};

/// One routing decision. An explicit next hop does not rewrite the Request-URI.
/// Route headers take precedence; use `with_request_uri` for a location lookup.
#[derive(Clone, Debug, Default)]
pub struct ProxyTarget {
    pub(crate) next_hop: Option<Uri>,
    pub(crate) request_uri: Option<Uri>,
}
impl ProxyTarget {
    /// Follow the request's Route set, or its Request-URI when there is no Route.
    pub fn routed() -> Self {
        Self::default()
    }
    pub fn next_hop(uri: Uri) -> Self {
        Self {
            next_hop: Some(uri),
            request_uri: None,
        }
    }
    pub fn with_request_uri(mut self, uri: Uri) -> Self {
        self.request_uri = Some(uri);
        self
    }
}

/// Advertised local loose route. Authorization and destination selection belong
/// to the caller; this is not an open relay, registrar or forking policy.
#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub(crate) local: Uri,
    pub(crate) record_route: bool,
}
impl ProxyConfig {
    pub fn new(mut local: Uri) -> crate::Result<Self> {
        address(&local).map_err(|(_, why)| crate::Error::EndpointError(why))?;
        if !local.has_lr() {
            local.params.push(Param::Lr);
        }
        Ok(Self {
            local,
            record_route: true,
        })
    }
    pub fn with_record_route(mut self, enabled: bool) -> Self {
        self.record_route = enabled;
        self
    }
    pub(super) fn local(&self, uri: &Uri) -> bool {
        self.local.host_with_port == uri.host_with_port && self.local.scheme == uri.scheme
    }
}

pub(super) fn prepare(
    original: &Request,
    target: ProxyTarget,
    config: &ProxyConfig,
    endpoint: &EndpointInnerRef,
) -> core::result::Result<(Request, SipAddr), (StatusCode, String)> {
    let bad = |error: crate::sip::Error| (StatusCode::BadRequest, error.to_string());
    let mut request = original.clone();
    request.call_id_header().map_err(bad)?;
    request.from_header().map_err(bad)?;
    request.to_header().map_err(bad)?;
    request.via_header().and_then(|h| h.typed()).map_err(bad)?;
    if request
        .cseq_header()
        .and_then(|h| h.method())
        .map_err(bad)?
        != request.method
    {
        return Err((StatusCode::BadRequest, "CSeq method mismatch".into()));
    }
    // Unsupported hop-by-hop requirements are not silently forwarded as if
    // implemented here. End-to-end Require/Supported are left untouched.
    if request
        .headers
        .iter()
        .any(|h| matches!(h, Header::ProxyRequire(v) if !v.value().trim().is_empty()))
    {
        return Err((StatusCode::BadExtension, "unsupported Proxy-Require".into()));
    }
    let hops = match request.max_forwards_header() {
        Ok(h) => {
            let value = h
                .value()
                .parse::<u32>()
                .map_err(|e| (StatusCode::BadRequest, e.to_string()))?;
            if value > 255 {
                return Err((StatusCode::BadRequest, "Max-Forwards exceeds 255".into()));
            }
            value
                .checked_sub(1)
                .ok_or((StatusCode::TooManyHops, "Max-Forwards exhausted".into()))?
        }
        Err(_) => 70,
    };
    request
        .headers
        .unique_push(Header::MaxForwards(hops.to_string().into()));
    let mut routes = Vec::new();
    for header in request.headers.iter() {
        if let Header::Route(route) = header {
            routes.extend(Route::parse_header_list(route.value()).map_err(bad)?);
        }
    }
    // Recover the remote target when a strict router placed our advertised
    // Record-Route URI in Request-URI (RFC 3261 16.4).
    if config.local(&request.uri) && request.uri.has_lr() && !routes.is_empty() {
        request.uri = routes.pop().expect("non-empty Route list").uri;
    }
    if routes.first().is_some_and(|r| config.local(&r.uri)) {
        routes.remove(0);
    }
    if let Some(uri) = target.request_uri {
        request.uri = uri;
    }
    let destination = routes
        .first()
        .map(|r| r.uri.clone())
        .or(target.next_hop)
        .unwrap_or_else(|| request.uri.clone());
    let destination = address(&destination)?;
    address(&request.uri)?;
    if original.uri.scheme == Some(Scheme::Sips)
        && (request.uri.scheme != Some(Scheme::Sips)
            || !matches!(
                destination.r#type,
                Some(Transport::Tls | Transport::Wss | Transport::TlsSctp)
            ))
    {
        return Err((
            StatusCode::Forbidden,
            "SIPS forwarding cannot downgrade transport".into(),
        ));
    }
    if routes.first().is_some_and(|r| !r.has_lr()) {
        let strict = routes.remove(0);
        routes.push(Route {
            display_name: None,
            uri: request.uri.clone(),
            params: vec![],
        });
        request.uri = strict.uri;
    }
    request.headers.retain(|h| !matches!(h, Header::Route(_)));
    request.headers.extend(
        routes
            .into_iter()
            .map(|r| Header::Route(r.to_string().into()))
            .collect(),
    );
    let initial = request.method == Method::Invite
        && request
            .to_header()
            .and_then(|h| h.tag())
            .map_err(bad)?
            .is_none();
    if initial && config.record_route {
        if original.uri.scheme == Some(Scheme::Sips) && config.local.scheme != Some(Scheme::Sips) {
            return Err((
                StatusCode::Forbidden,
                "SIPS requires a secure advertised Record-Route".into(),
            ));
        }
        request
            .headers
            .push_front(Header::RecordRoute(format!("<{}>", config.local).into()));
    }
    let mut advertised =
        SipAddr::try_from(&config.local).map_err(|e| (StatusCode::BadRequest, e.to_string()))?;
    advertised.r#type = destination.r#type;
    let via = endpoint
        .get_via(Some(advertised), None)
        .map_err(|e| (StatusCode::BadRequest, e.to_string()))?;
    request.headers.push_front(via.into());
    Ok((request, destination))
}

fn address(uri: &Uri) -> core::result::Result<SipAddr, (StatusCode, String)> {
    if !matches!(uri.scheme, Some(Scheme::Sip | Scheme::Sips)) {
        return Err((
            StatusCode::UnsupportedUriScheme,
            "proxy requires a SIP or SIPS URI".into(),
        ));
    }
    let mut address =
        SipAddr::try_from(uri).map_err(|e| (StatusCode::BadRequest, e.to_string()))?;
    if uri.scheme == Some(Scheme::Sips) {
        match address.r#type {
            None | Some(Transport::Tcp | Transport::Tls) => address.r#type = Some(Transport::Tls),
            Some(Transport::Wss | Transport::TlsSctp) => {}
            _ => {
                return Err((
                    StatusCode::Forbidden,
                    "SIPS requires a secure transport".into(),
                ))
            }
        }
        if address.addr.port.is_none() {
            address.addr.port = Some(5061.into());
        }
    }
    Ok(address)
}

/// Remove precisely the forwarding hop, including a comma-folded Via list.
pub(super) fn pop_via(response: &mut Response, request: &Request) -> crate::Result<()> {
    let first = response.via_header()?.first_value()?;
    if first.typed()?.branch() != request.via_header()?.typed()?.branch() {
        return Err(crate::Error::EndpointError(
            "proxy response Via mismatch".into(),
        ));
    }
    let mut removed = false;
    let mut headers = Vec::new();
    for header in response.headers.iter() {
        if let Header::Via(via) = header {
            if !removed {
                removed = true;
                let first = via.first_value()?;
                let rest = via.value().trim()[first.value().len()..].trim_start();
                if let Some(rest) = rest.strip_prefix(',') {
                    headers.push(Header::Via(rest.trim_start().into()));
                }
                continue;
            }
        }
        headers.push(header.clone());
    }
    response.headers = headers.into();
    response.via_header()?;
    Ok(())
}

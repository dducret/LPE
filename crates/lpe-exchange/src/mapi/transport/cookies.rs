use axum::http::HeaderMap;

use super::{
    get_session, mapi_payload_fingerprint, AccountPrincipal, MapiEndpoint, EMSMDB_COOKIE,
    EMSMDB_COOKIE_PATH, EMSMDB_SEQUENCE_COOKIE, MAPI_SESSION_MAX_AGE_SECONDS, NSPI_COOKIE,
    NSPI_COOKIE_PATH, NSPI_SEQUENCE_COOKIE,
};

pub(in crate::mapi) fn request_cookie(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> Option<String> {
    request_named_cookie(cookie_name(endpoint), headers)
}

pub(in crate::mapi) fn request_sequence_cookie(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> Option<String> {
    request_named_cookie(sequence_cookie_name(endpoint), headers)
}

pub(in crate::mapi) fn request_sequence_cookie_matches(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    session_id: &str,
) -> bool {
    match request_sequence_cookie(endpoint, headers) {
        Some(sequence_id) => sequence_id == session_id,
        None => true,
    }
}

pub(in crate::mapi) fn request_named_cookie(name: &str, headers: &HeaderMap) -> Option<String> {
    request_named_cookie_candidates(name, headers)
        .last()
        .cloned()
}

fn request_named_cookie_candidates(name: &str, headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|cookie| {
            cookie
                .split(';')
                .filter_map(|part| {
                    let (key, value) = part.trim().split_once('=')?;
                    (key == name && !value.is_empty()).then(|| value.to_string())
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::mapi) struct CookieValueDebug {
    pub(in crate::mapi) suffix: String,
    pub(in crate::mapi) hash: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::mapi) struct SessionCookieLookupDebug {
    pub(in crate::mapi) cookie_header_count: usize,
    pub(in crate::mapi) context_candidate_count: usize,
    pub(in crate::mapi) sequence_candidate_count: usize,
    pub(in crate::mapi) selected_context: CookieValueDebug,
    pub(in crate::mapi) selected_sequence: CookieValueDebug,
    pub(in crate::mapi) selected_session_exists: bool,
    pub(in crate::mapi) selected_session_endpoint_matches: bool,
    pub(in crate::mapi) selected_session_principal_matches: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RequestCookieTransportDebug {
    pub(crate) cookie_header_count: usize,
    pub(crate) context_candidate_count: usize,
    pub(crate) sequence_candidate_count: usize,
    pub(crate) selected_context_suffix: String,
    pub(crate) selected_context_hash: String,
    pub(crate) selected_sequence_suffix: String,
    pub(crate) selected_sequence_hash: String,
}

pub(crate) fn request_cookie_transport_debug(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> RequestCookieTransportDebug {
    let context_candidates = request_named_cookie_candidates(cookie_name(endpoint), headers);
    let sequence_candidates =
        request_named_cookie_candidates(sequence_cookie_name(endpoint), headers);
    let selected_context = context_candidates.last().cloned();
    let selected_sequence = sequence_candidates.last().cloned();
    let selected_context = cookie_value_debug(selected_context.as_deref());
    let selected_sequence = cookie_value_debug(selected_sequence.as_deref());

    RequestCookieTransportDebug {
        cookie_header_count: headers.get_all("cookie").iter().count(),
        context_candidate_count: context_candidates.len(),
        sequence_candidate_count: sequence_candidates.len(),
        selected_context_suffix: selected_context.suffix,
        selected_context_hash: selected_context.hash,
        selected_sequence_suffix: selected_sequence.suffix,
        selected_sequence_hash: selected_sequence.hash,
    }
}

pub(in crate::mapi) fn session_cookie_lookup_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
) -> SessionCookieLookupDebug {
    let context_candidates = request_named_cookie_candidates(cookie_name(endpoint), headers);
    let sequence_candidates =
        request_named_cookie_candidates(sequence_cookie_name(endpoint), headers);
    let selected_context = context_candidates.last().cloned();
    let selected_sequence = sequence_candidates.last().cloned();
    let session = selected_context.as_deref().and_then(get_session);
    let selected_session_exists = session.is_some();
    let selected_session_endpoint_matches = session
        .as_ref()
        .is_some_and(|session| session.endpoint == endpoint);
    let selected_session_principal_matches = session.as_ref().is_some_and(|session| {
        session.tenant_id == principal.tenant_id
            && session.account_id == principal.account_id
            && session.email == principal.email
    });

    SessionCookieLookupDebug {
        cookie_header_count: headers.get_all("cookie").iter().count(),
        context_candidate_count: context_candidates.len(),
        sequence_candidate_count: sequence_candidates.len(),
        selected_context: cookie_value_debug(selected_context.as_deref()),
        selected_sequence: cookie_value_debug(selected_sequence.as_deref()),
        selected_session_exists,
        selected_session_endpoint_matches,
        selected_session_principal_matches,
    }
}

pub(in crate::mapi) fn cookie_value_debug(value: Option<&str>) -> CookieValueDebug {
    let Some(value) = value else {
        return CookieValueDebug::default();
    };
    CookieValueDebug {
        suffix: cookie_value_suffix(value),
        hash: format!("{:016x}", mapi_payload_fingerprint(value.as_bytes())),
    }
}

pub(in crate::mapi) fn cookie_value_suffix(value: &str) -> String {
    let mut chars = value.chars().rev().take(8).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

pub(in crate::mapi) fn log_session_cookie_lookup(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_type: &str,
) {
    let summary = session_cookie_lookup_debug(endpoint, principal, headers);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };

    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        cookie_header_count = summary.cookie_header_count,
        mapi_context_candidate_count = summary.context_candidate_count,
        mapi_sequence_candidate_count = summary.sequence_candidate_count,
        selected_context_suffix = %summary.selected_context.suffix,
        selected_context_hash = %summary.selected_context.hash,
        selected_sequence_suffix = %summary.selected_sequence.suffix,
        selected_sequence_hash = %summary.selected_sequence.hash,
        selected_session_exists = summary.selected_session_exists,
        selected_session_endpoint_matches = summary.selected_session_endpoint_matches,
        selected_session_principal_matches = summary.selected_session_principal_matches,
        message = "rca debug mapi session cookie lookup",
    );
}

pub(in crate::mapi) fn session_cookie(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> String {
    context_cookie(endpoint, cookie_name(endpoint), session_id, expired)
}

pub(in crate::mapi) fn sequence_cookie(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> String {
    context_cookie(
        endpoint,
        sequence_cookie_name(endpoint),
        session_id,
        expired,
    )
}

pub(in crate::mapi) fn session_context_cookies(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> Vec<String> {
    vec![
        session_cookie(endpoint, session_id, expired),
        sequence_cookie(endpoint, session_id, expired),
    ]
}

pub(in crate::mapi) fn context_cookie(
    endpoint: MapiEndpoint,
    name: &str,
    session_id: &str,
    expired: bool,
) -> String {
    let path = cookie_path(endpoint);
    if expired {
        format!("{name}=; Path={path}; Max-Age=0; HttpOnly; SameSite=Lax; Secure")
    } else {
        format!(
            "{name}={session_id}; Path={path}; Max-Age={MAPI_SESSION_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax; Secure"
        )
    }
}

pub(in crate::mapi) fn cookie_name(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE,
        MapiEndpoint::Nspi => NSPI_COOKIE,
    }
}

pub(in crate::mapi) fn sequence_cookie_name(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_SEQUENCE_COOKIE,
        MapiEndpoint::Nspi => NSPI_SEQUENCE_COOKIE,
    }
}

pub(in crate::mapi) fn cookie_path(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE_PATH,
        MapiEndpoint::Nspi => NSPI_COOKIE_PATH,
    }
}

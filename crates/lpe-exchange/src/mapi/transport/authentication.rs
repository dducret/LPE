use super::*;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::OnceLock;

type HmacSha256 = Hmac<Sha256>;

const CREDENTIAL_FINGERPRINT_DOMAIN: &[u8] = b"lpe-mapi-credential-v1";
const VERIFIER_FINGERPRINT_DOMAIN: &[u8] = b"lpe-mapi-verifier-v1";

static MAPI_AUTHENTICATION_HMAC_KEY: OnceLock<[u8; 32]> = OnceLock::new();

pub(super) struct AuthenticatedMapiRequest {
    pub(super) verified: VerifiedAccountAuthentication,
    pub(super) session_authentication: MapiSessionAuthentication,
    reconnect_context: Option<AccountPrincipal>,
}

pub(super) enum MapiAuthenticationOutcome {
    Accepted(AuthenticatedMapiRequest),
    Rejected {
        principal: AccountPrincipal,
        response: Response,
    },
}

enum PresentedCredential {
    Basic { fingerprint: [u8; 32] },
    Bearer { fingerprint: [u8; 32] },
}

enum HttpSessionLookup {
    Missing,
    Valid(String, MapiSessionAuthentication),
    Invalid(AccountPrincipal),
}

impl PresentedCredential {
    fn fingerprint(&self) -> &[u8; 32] {
        match self {
            Self::Basic { fingerprint } | Self::Bearer { fingerprint } => fingerprint,
        }
    }
}

pub(super) async fn authenticate_mapi_request<S>(
    store: &S,
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    request_type: &MapiRequestType,
    request_id: &str,
) -> Result<MapiAuthenticationOutcome>
where
    S: ExchangeStore,
{
    let establishment = is_session_establishment(endpoint, request_type);
    if !establishment && consumes_session_context(endpoint, request_type) {
        match http_session_authentication(endpoint, headers) {
            HttpSessionLookup::Valid(session_id, stored_authentication) => {
                return authenticate_session_continuation(
                    store,
                    headers,
                    request_type,
                    request_id,
                    &session_id,
                    &stored_authentication,
                )
                .await;
            }
            HttpSessionLookup::Invalid(principal) => {
                record_session_login_failure(
                    store,
                    &principal,
                    &principal.email,
                    "authentication-context-changed",
                )
                .await;
                return Ok(rejected_continuation(
                    request_type,
                    request_id,
                    &principal,
                    "MAPI authentication context changed",
                ));
            }
            HttpSessionLookup::Missing => {}
        }
    }

    let mut verified = verify_account_authentication(store, None, headers, "mapi").await?;
    refresh_opaque_session_principal(store, &mut verified).await?;
    let session_authentication = session_authentication(&verified, headers)?;
    let reconnect_context = establishment
        .then(|| reconnect_context(endpoint, headers, &session_authentication))
        .flatten();

    Ok(MapiAuthenticationOutcome::Accepted(
        AuthenticatedMapiRequest {
            verified,
            session_authentication,
            reconnect_context,
        },
    ))
}

pub(super) async fn record_mapi_establishment_result<S>(
    store: &S,
    endpoint: MapiEndpoint,
    request_type: &MapiRequestType,
    authentication: &AuthenticatedMapiRequest,
    response_code: Option<&str>,
) where
    S: ExchangeStore,
{
    if !is_session_establishment(endpoint, request_type) {
        return;
    }
    match response_code {
        Some("0") => {
            record_account_login_success(store, &authentication.verified, "mapi").await;
        }
        Some("10") => {
            if let Some(previous) = authentication.reconnect_context.as_ref() {
                record_session_login_failure(
                    store,
                    previous,
                    &previous.email,
                    "authentication-context-changed",
                )
                .await;
            }
        }
        _ => {}
    }
}

fn is_session_establishment(endpoint: MapiEndpoint, request_type: &MapiRequestType) -> bool {
    matches!(
        (endpoint, request_type),
        (MapiEndpoint::Emsmdb, MapiRequestType::Connect)
            | (MapiEndpoint::Nspi, MapiRequestType::Bind)
    )
}

fn consumes_session_context(endpoint: MapiEndpoint, request_type: &MapiRequestType) -> bool {
    match endpoint {
        MapiEndpoint::Emsmdb => matches!(
            request_type,
            MapiRequestType::Execute
                | MapiRequestType::Disconnect
                | MapiRequestType::NotificationWait
                | MapiRequestType::Ping
        ),
        MapiEndpoint::Nspi => {
            matches!(
                request_type,
                MapiRequestType::Unbind | MapiRequestType::Ping
            ) || request_type.requires_nspi_session()
        }
    }
}

fn http_session_authentication(endpoint: MapiEndpoint, headers: &HeaderMap) -> HttpSessionLookup {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return HttpSessionLookup::Missing;
    };
    let Some(session) = get_session(&session_id) else {
        return HttpSessionLookup::Missing;
    };
    let authentication = session.authentication.clone();
    if let Some(authentication) = authentication {
        if mapi_http_session_matches(&session, endpoint, &authentication.principal) {
            return HttpSessionLookup::Valid(session_id, authentication);
        }
    }
    HttpSessionLookup::Invalid(AccountPrincipal {
        tenant_id: session.tenant_id,
        account_id: session.account_id,
        email: session.email,
        display_name: session
            .authentication
            .map(|authentication| authentication.principal.display_name)
            .unwrap_or_default(),
        quota_mb: None,
        quota_used_octets: None,
    })
}

async fn authenticate_session_continuation<S>(
    store: &S,
    headers: &HeaderMap,
    request_type: &MapiRequestType,
    request_id: &str,
    session_id: &str,
    stored: &MapiSessionAuthentication,
) -> Result<MapiAuthenticationOutcome>
where
    S: ExchangeStore,
{
    let presented = match presented_credential(headers) {
        Ok(presented) => presented,
        Err(error) => {
            let subject = if headers.contains_key("authorization") {
                "invalid-authentication-header"
            } else {
                "missing-authentication"
            };
            record_session_login_failure(
                store,
                &stored.principal,
                &stored.principal.email,
                subject,
            )
            .await;
            return Ok(rejected_continuation(
                request_type,
                request_id,
                &stored.principal,
                &error.to_string(),
            ));
        }
    };

    if !stored.credential_fingerprint_matches(presented.fingerprint()) {
        record_session_login_failure(
            store,
            &stored.principal,
            &stored.principal.email,
            "authentication-context-changed",
        )
        .await;
        return Ok(rejected_continuation(
            request_type,
            request_id,
            &stored.principal,
            "MAPI authentication context changed",
        ));
    }

    let refreshed = match refresh_matching_authentication(store, headers, stored, &presented).await
    {
        Ok(authentication) => authentication,
        Err(subject) => {
            record_session_login_failure(
                store,
                &stored.principal,
                &stored.principal.email,
                subject,
            )
            .await;
            return Ok(rejected_continuation(
                request_type,
                request_id,
                &stored.principal,
                "MAPI authentication context is no longer valid",
            ));
        }
    };

    if !update_session_authentication(session_id, &refreshed.session_authentication) {
        return Ok(rejected_continuation(
            request_type,
            request_id,
            &stored.principal,
            "MAPI session context not found",
        ));
    }

    Ok(MapiAuthenticationOutcome::Accepted(refreshed))
}

async fn refresh_matching_authentication<S>(
    store: &S,
    headers: &HeaderMap,
    stored: &MapiSessionAuthentication,
    presented: &PresentedCredential,
) -> std::result::Result<AuthenticatedMapiRequest, &'static str>
where
    S: ExchangeStore,
{
    let verified = match stored.method {
        AccountAuthenticationMethod::Password | AccountAuthenticationMethod::AppPassword => {
            refresh_basic_authentication(store, stored).await?
        }
        AccountAuthenticationMethod::Session | AccountAuthenticationMethod::OAuth => {
            let mut verified = verify_account_authentication(store, None, headers, "mapi")
                .await
                .map_err(|_| "credential-revoked")?;
            refresh_opaque_session_principal(store, &mut verified)
                .await
                .map_err(|_| "credential-revoked")?;
            verified
        }
    };
    if verified.method != stored.method || !same_principal(&verified.principal, &stored.principal) {
        return Err("authentication-context-changed");
    }
    let session_authentication =
        session_authentication_with_credential_fingerprint(&verified, *presented.fingerprint());
    if !stored.credential_matches(&session_authentication) {
        return Err("credential-revoked");
    }
    Ok(AuthenticatedMapiRequest {
        verified,
        session_authentication,
        reconnect_context: None,
    })
}

async fn refresh_basic_authentication<S>(
    store: &S,
    stored: &MapiSessionAuthentication,
) -> std::result::Result<VerifiedAccountAuthentication, &'static str>
where
    S: ExchangeStore,
{
    let login = store
        .fetch_account_login(&stored.principal.email)
        .await
        .map_err(|_| "credential-state-unavailable")?
        .ok_or("credential-revoked")?;
    if login.status != "active" {
        return Err("inactive-account");
    }
    let principal = AccountPrincipal {
        tenant_id: login.tenant_id,
        account_id: login.account_id,
        email: login.email.clone(),
        display_name: login.display_name,
        quota_mb: Some(login.quota_mb),
        quota_used_octets: Some(login.quota_used_octets),
    };
    if !same_principal(&principal, &stored.principal) {
        return Err("authentication-context-changed");
    }
    let verifier = match stored.method {
        AccountAuthenticationMethod::Password => {
            AccountAuthenticationVerifier::PasswordHash(login.password_hash)
        }
        AccountAuthenticationMethod::AppPassword => {
            let id = stored.app_password_id.ok_or("credential-revoked")?;
            let app_password = store
                .fetch_active_account_app_passwords(&login.email)
                .await
                .map_err(|_| "credential-state-unavailable")?
                .into_iter()
                .find(|entry| entry.id == id)
                .ok_or("credential-revoked")?;
            AccountAuthenticationVerifier::AppPassword {
                id,
                password_hash: app_password.password_hash,
            }
        }
        _ => return Err("authentication-context-changed"),
    };
    Ok(VerifiedAccountAuthentication {
        principal,
        method: stored.method,
        verifier,
    })
}

async fn refresh_opaque_session_principal<S>(
    store: &S,
    verified: &mut VerifiedAccountAuthentication,
) -> Result<()>
where
    S: ExchangeStore,
{
    if verified.method != AccountAuthenticationMethod::Session {
        return Ok(());
    }
    let Some(login) = store.fetch_account_login(&verified.principal.email).await? else {
        return Ok(());
    };
    if login.status != "active"
        || login.tenant_id != verified.principal.tenant_id
        || login.account_id != verified.principal.account_id
        || login.email != verified.principal.email
    {
        return Err(anyhow!("invalid credentials"));
    }
    verified.principal = AccountPrincipal {
        tenant_id: login.tenant_id,
        account_id: login.account_id,
        email: login.email,
        display_name: login.display_name,
        quota_mb: Some(login.quota_mb),
        quota_used_octets: Some(login.quota_used_octets),
    };
    Ok(())
}

fn presented_credential(headers: &HeaderMap) -> Result<PresentedCredential> {
    if let Some(token) = bearer_token(headers) {
        return Ok(PresentedCredential::Bearer {
            fingerprint: keyed_fingerprint(
                CREDENTIAL_FINGERPRINT_DOMAIN,
                &[b"bearer", token.as_bytes()],
            ),
        });
    }
    if let Some((username, password)) = basic_credentials(headers)? {
        let normalized_login = normalize_login_name(&username, None);
        return Ok(PresentedCredential::Basic {
            fingerprint: keyed_fingerprint(
                CREDENTIAL_FINGERPRINT_DOMAIN,
                &[b"basic", normalized_login.as_bytes(), password.as_bytes()],
            ),
        });
    }
    if headers.contains_key("authorization") {
        return Err(anyhow!("invalid account authentication header"));
    }
    Err(anyhow!("missing account authentication"))
}

fn session_authentication(
    verified: &VerifiedAccountAuthentication,
    headers: &HeaderMap,
) -> Result<MapiSessionAuthentication> {
    let presented = presented_credential(headers)?;
    Ok(session_authentication_with_credential_fingerprint(
        verified,
        *presented.fingerprint(),
    ))
}

fn session_authentication_with_credential_fingerprint(
    verified: &VerifiedAccountAuthentication,
    credential_fingerprint: [u8; 32],
) -> MapiSessionAuthentication {
    let (app_password_id, verifier_fingerprint) = match &verified.verifier {
        AccountAuthenticationVerifier::None => (None, None),
        AccountAuthenticationVerifier::PasswordHash(password_hash) => (
            None,
            Some(keyed_fingerprint(
                VERIFIER_FINGERPRINT_DOMAIN,
                &[b"password", password_hash.as_bytes()],
            )),
        ),
        AccountAuthenticationVerifier::AppPassword { id, password_hash } => (
            Some(*id),
            Some(keyed_fingerprint(
                VERIFIER_FINGERPRINT_DOMAIN,
                &[b"app-password", id.as_bytes(), password_hash.as_bytes()],
            )),
        ),
    };
    MapiSessionAuthentication {
        principal: verified.principal.clone(),
        method: verified.method,
        app_password_id,
        credential_fingerprint,
        verifier_fingerprint,
    }
}

fn reconnect_context(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    authentication: &MapiSessionAuthentication,
) -> Option<AccountPrincipal> {
    let session_id = request_cookie(endpoint, headers)?;
    let session = get_session(&session_id)?;
    let changed = !mapi_http_session_matches(&session, endpoint, &authentication.principal)
        || !session
            .authentication
            .as_ref()
            .is_some_and(|current| current.credential_matches(authentication));
    changed.then(|| {
        session
            .authentication
            .map(|current| current.principal)
            .unwrap_or(AccountPrincipal {
                tenant_id: session.tenant_id,
                account_id: session.account_id,
                email: session.email,
                display_name: String::new(),
                quota_mb: None,
                quota_used_octets: None,
            })
    })
}

fn rejected_continuation(
    request_type: &MapiRequestType,
    request_id: &str,
    principal: &AccountPrincipal,
    message: &str,
) -> MapiAuthenticationOutcome {
    MapiAuthenticationOutcome::Rejected {
        principal: principal.clone(),
        response: mapi_diagnostic_response(request_type.header_value(), request_id, 10, message),
    }
}

async fn record_session_login_failure<S>(
    store: &S,
    session_principal: &AccountPrincipal,
    actor: &str,
    subject: &str,
) where
    S: ExchangeStore,
{
    let _ = store
        .append_audit_event(
            &session_principal.tenant_id,
            AuditEntryInput {
                actor: actor.to_string(),
                action: "mail-auth.mapi.login-failed".to_string(),
                subject: subject.to_string(),
            },
        )
        .await;
}

fn same_principal(left: &AccountPrincipal, right: &AccountPrincipal) -> bool {
    left.tenant_id == right.tenant_id
        && left.account_id == right.account_id
        && left.email == right.email
}

fn keyed_fingerprint(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(mapi_authentication_hmac_key())
        .expect("HMAC-SHA256 accepts keys of any size");
    mac.update(domain);
    for part in parts {
        mac.update(&(part.len() as u64).to_le_bytes());
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn mapi_authentication_hmac_key() -> &'static [u8; 32] {
    MAPI_AUTHENTICATION_HMAC_KEY.get_or_init(|| {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(first.as_bytes());
        key[16..].copy_from_slice(second.as_bytes());
        key
    })
}

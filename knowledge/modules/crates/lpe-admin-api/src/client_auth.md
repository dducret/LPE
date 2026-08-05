---
type: Rust Module
title: client_auth
resource: crates/lpe-admin-api/src/client_auth.rs#L1-L517
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-query-state-http-headermap-statuscode-response-redirect-json
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/lpe-mail-auth-issue-oauth-access-token-normalize-scope-default-oauth-access-scope
  - external/lpe-storage-auditentryinput-authenticatedaccount-healthresponse-storage
  - external/crate-account-oidc-http-bad-request-error-internal-error-public-origin-require-account-security-client-oauth-access-token-seconds-client-session-minutes-generate-app-password-secret-hash-password-verify-password-totp-types-accountapppasswordsresponse-accountauthfactorsresponse-apiresult-clientloginresponse-clientoauthaccesstokenresponse-clientoidcmetadataresponse-clientoidcstartresponse-createaccountapppasswordrequest-createaccountapppasswordresponse-createclientoauthaccesstokenrequest-enrolltotprequest-enrolltotpresponse-loginrequest-verifytotprequest
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [client_login](../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [client_logout](../../../../functions/crates/lpe-admin-api/src/client_auth/client_logout.md)
- [client_me](../../../../functions/crates/lpe-admin-api/src/client_auth/client_me.md)
- [account_auth_factors](../../../../functions/crates/lpe-admin-api/src/client_auth/account_auth_factors.md)
- [enroll_account_totp](../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)
- [verify_account_totp_factor](../../../../functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor.md)
- [revoke_account_factor](../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor.md)
- [list_account_app_passwords](../../../../functions/crates/lpe-admin-api/src/client_auth/list_account_app_passwords.md)
- [create_account_app_password](../../../../functions/crates/lpe-admin-api/src/client_auth/create_account_app_password.md)
- [revoke_account_app_password](../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_app_password.md)
- [create_client_oauth_access_token](../../../../functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token.md)
- [client_oidc_metadata](../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_metadata.md)
- [client_oidc_start](../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_start.md)
- [client_oidc_callback](../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::Redirect,
    Json,
}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `lpe_mail_auth::{issue_oauth_access_token, normalize_scope, DEFAULT_OAUTH_ACCESS_SCOPE}`
- `lpe_storage::{AuditEntryInput, AuthenticatedAccount, HealthResponse, Storage}`
- `crate::{
    account_oidc,
    http::{bad_request_error, internal_error, public_origin},
    require_account,
    security::{
        client_oauth_access_token_seconds, client_session_minutes, generate_app_password_secret,
        hash_password, verify_password,
    },
    totp,
    types::{
        AccountAppPasswordsResponse, AccountAuthFactorsResponse, ApiResult, ClientLoginResponse,
        ClientOauthAccessTokenResponse, ClientOidcMetadataResponse, ClientOidcStartResponse,
        CreateAccountAppPasswordRequest, CreateAccountAppPasswordResponse,
        CreateClientOauthAccessTokenRequest, EnrollTotpRequest, EnrollTotpResponse, LoginRequest,
        VerifyTotpRequest,
    },
}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
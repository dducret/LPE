---
type: Rust Module
title: admin_auth
resource: crates/lpe-admin-api/src/admin_auth.rs#L1-L399
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-query-state-http-headermap-statuscode-response-redirect-json
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/lpe-storage-auditentryinput-authenticatedadmin-healthresponse-newadminauthfactor-storage
  - external/crate-http-bad-request-error-bearer-token-internal-error-public-origin-oidc-require-admin-security-admin-session-minutes-verify-password-totp-types-adminauthfactorsresponse-apiresult-enrolltotprequest-enrolltotpresponse-loginrequest-loginresponse-oidcmetadataresponse-oidcstartresponse-verifytotprequest
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [login](../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [logout](../../../../functions/crates/lpe-admin-api/src/admin_auth/logout.md)
- [me](../../../../functions/crates/lpe-admin-api/src/admin_auth/me.md)
- [admin_auth_factors](../../../../functions/crates/lpe-admin-api/src/admin_auth/admin_auth_factors.md)
- [enroll_totp](../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)
- [verify_totp_factor](../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)
- [revoke_admin_factor](../../../../functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor.md)
- [oidc_metadata](../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_metadata.md)
- [oidc_start](../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_start.md)
- [oidc_callback](../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::Redirect,
    Json,
}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `lpe_storage::{
    AuditEntryInput, AuthenticatedAdmin, HealthResponse, NewAdminAuthFactor, Storage,
}`
- `crate::{
    http::{bad_request_error, bearer_token, internal_error, public_origin},
    oidc, require_admin,
    security::{admin_session_minutes, verify_password},
    totp,
    types::{
        AdminAuthFactorsResponse, ApiResult, EnrollTotpRequest, EnrollTotpResponse, LoginRequest,
        LoginResponse, OidcMetadataResponse, OidcStartResponse, VerifyTotpRequest,
    },
}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
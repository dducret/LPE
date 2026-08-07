---
type: Rust Module
title: http
resource: crates/lpe-admin-api/src/http.rs#L1-L70
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-headermap-statuscode
  - external/std-env
  - external/super-account-session-token
  - external/axum-http-headermap-headervalue
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [internal_error](../../../../functions/crates/lpe-admin-api/src/http/internal_error.md)
- [bad_request_error](../../../../functions/crates/lpe-admin-api/src/http/bad_request_error.md)
- [bearer_token](../../../../functions/crates/lpe-admin-api/src/http/bearer_token.md)
- [account_session_token](../../../../functions/crates/lpe-admin-api/src/http/account_session_token.md)
- [public_origin](../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)
- [forwarded_header](../../../../functions/crates/lpe-admin-api/src/http/forwarded_header.md)
- [account_session_cookie_authenticates_without_a_browser_set_authorization_header](../../../../functions/crates/lpe-admin-api/src/http/account_session_cookie_authenticates_without_a_browser_set_authorization_header.md)

# Imports

- `axum::http::{HeaderMap, StatusCode}`
- `std::env`
- `super::account_session_token`
- `axum::http::{HeaderMap, HeaderValue}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
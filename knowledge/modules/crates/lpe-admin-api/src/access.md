---
type: Rust Module
title: access
resource: crates/lpe-admin-api/src/access.rs#L1-L53
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-headermap-statuscode
  - external/lpe-storage-authenticatedaccount-authenticatedadmin-storage
  - external/crate-http-account-session-token-bearer-token-internal-error
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [require_admin](../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [require_account](../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [admin_has_right](../../../../functions/crates/lpe-admin-api/src/access/admin_has_right.md)

# Imports

- `axum::http::{HeaderMap, StatusCode}`
- `lpe_storage::{AuthenticatedAccount, AuthenticatedAdmin, Storage}`
- `crate::http::{account_session_token, bearer_token, internal_error}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
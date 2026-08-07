---
type: Rust Function
title: account_session_token
resource: crates/lpe-admin-api/src/http.rs#L21-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/client_auth/client_logout
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
---

# Signature

`pub(crate) fn account_session_token(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [client_logout](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_logout.md)
- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)
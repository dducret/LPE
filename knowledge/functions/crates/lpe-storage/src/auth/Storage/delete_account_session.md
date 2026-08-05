---
type: Rust Method
title: delete_account_session
resource: crates/lpe-storage/src/auth.rs#L1214-L1239
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_logout
---

# Signature

`pub async fn delete_account_session(&self, token: &str) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [client_logout](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_logout.md)
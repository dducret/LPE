---
type: Rust Method
title: delete_admin_session
resource: crates/lpe-storage/src/auth.rs#L1136-L1161
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/logout
---

# Signature

`pub async fn delete_admin_session(&self, token: &str) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [logout](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/logout.md)
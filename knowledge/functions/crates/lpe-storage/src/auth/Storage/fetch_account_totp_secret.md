---
type: Rust Method
title: fetch_account_totp_secret
resource: crates/lpe-storage/src/auth.rs#L553-L586
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_login
---

# Signature

`pub async fn fetch_account_totp_secret( &self, account_email: &str, ) -> Result<Option<(Uuid, String)>>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)

# Called by

- [client_login](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
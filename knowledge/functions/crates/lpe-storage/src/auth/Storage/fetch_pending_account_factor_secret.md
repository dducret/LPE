---
type: Rust Method
title: fetch_pending_account_factor_secret
resource: crates/lpe-storage/src/auth.rs#L588-L612
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor
---

# Signature

`pub async fn fetch_pending_account_factor_secret( &self, account_email: &str, factor_id: Uuid, ) -> Result<Option<String>>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)

# Called by

- [verify_account_totp_factor](../../../../../../functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor.md)
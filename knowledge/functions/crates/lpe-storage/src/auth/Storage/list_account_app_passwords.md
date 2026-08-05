---
type: Rust Method
title: list_account_app_passwords
resource: crates/lpe-storage/src/auth.rs#L694-L732
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
---

# Signature

`pub async fn list_account_app_passwords( &self, account_email: &str, ) -> Result<Vec<AccountAppPassword>>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
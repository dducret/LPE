---
type: Rust Method
title: fetch_account_login
resource: crates/lpe-storage/src/auth.rs#L989-L1038
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
---

# Signature

`pub async fn fetch_account_login(&self, email: &str) -> Result<Option<AccountLogin>>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
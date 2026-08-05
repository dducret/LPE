---
type: Rust Function
title: domain_from_email
resource: crates/lpe-storage/src/util.rs#L55-L60
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
---

# Signature

`pub(crate) fn domain_from_email(email: &str) -> Result<String>`

# Called by

- [tenant_id_for_account_email](../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
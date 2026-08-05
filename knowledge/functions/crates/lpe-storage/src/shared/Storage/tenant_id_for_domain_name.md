---
type: Rust Method
title: tenant_id_for_domain_name
resource: crates/lpe-storage/src/shared.rs#L697-L714
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
---

# Signature

`pub(crate) async fn tenant_id_for_domain_name(&self, domain_name: &str) -> Result<Uuid>`

# Called by

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
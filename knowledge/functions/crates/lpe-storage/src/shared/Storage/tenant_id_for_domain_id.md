---
type: Rust Method
title: tenant_id_for_domain_id
resource: crates/lpe-storage/src/shared.rs#L716-L729
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/create_server_administrator
---

# Signature

`pub(crate) async fn tenant_id_for_domain_id(&self, domain_id: Uuid) -> Result<Uuid>`

# Called by

- [create_server_administrator](../../../../../../functions/crates/lpe-storage/src/admin/Storage/create_server_administrator.md)
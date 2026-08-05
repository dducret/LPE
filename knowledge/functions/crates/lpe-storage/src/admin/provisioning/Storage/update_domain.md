---
type: Rust Method
title: update_domain
resource: crates/lpe-storage/src/admin/provisioning.rs#L336-L367
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn update_domain(&self, input: UpdateDomain, audit: AuditEntryInput) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
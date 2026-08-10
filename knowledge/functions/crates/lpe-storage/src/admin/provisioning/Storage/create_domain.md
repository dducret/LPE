---
type: Rust Method
title: create_domain
resource: crates/lpe-storage/src/admin/provisioning.rs#L303-L334
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn create_domain(&self, input: NewDomain, audit: AuditEntryInput) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
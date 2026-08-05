---
type: Rust Method
title: append_audit_event
resource: crates/lpe-storage/src/admin.rs#L120-L125
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn append_audit_event(&self, tenant_id: Uuid, audit: AuditEntryInput) -> Result<()>`

# Calls

- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
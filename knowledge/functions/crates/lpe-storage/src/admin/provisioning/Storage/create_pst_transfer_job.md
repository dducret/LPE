---
type: Rust Method
title: create_pst_transfer_job
resource: crates/lpe-storage/src/admin/provisioning.rs#L236-L301
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn create_pst_transfer_job( &self, input: NewPstTransferJob, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
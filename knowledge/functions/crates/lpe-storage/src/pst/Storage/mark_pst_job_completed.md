---
type: Rust Method
title: mark_pst_job_completed
resource: crates/lpe-storage/src/pst.rs#L165-L188
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs
---

# Signature

`async fn mark_pst_job_completed( &self, tenant_id: &Uuid, job_id: Uuid, processed_messages: u32, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [process_pending_pst_jobs](../../../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)
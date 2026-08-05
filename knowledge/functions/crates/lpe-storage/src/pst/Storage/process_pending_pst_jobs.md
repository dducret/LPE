---
type: Rust Method
title: process_pending_pst_jobs
resource: crates/lpe-storage/src/pst.rs#L82-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_running
  - functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_failed
  - functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst
  - functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst
  - functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_completed
  called_by:
  - functions/crates/lpe-admin-api/src/console/run_pst_jobs
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_pst_path
---

# Signature

`pub async fn process_pending_pst_jobs(&self) -> Result<PstJobExecutionSummary>`

# Calls

- [mark_pst_job_running](../../../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_running.md)
- [mark_pst_job_failed](../../../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_failed.md)
- [export_mailbox_to_pst](../../../../../../functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst.md)
- [import_mailbox_from_pst](../../../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)
- [mark_pst_job_completed](../../../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_completed.md)

# Called by

- [run_pst_jobs](../../../../../../functions/crates/lpe-admin-api/src/console/run_pst_jobs.md)
- [exercise_pst_path](../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_pst_path.md)
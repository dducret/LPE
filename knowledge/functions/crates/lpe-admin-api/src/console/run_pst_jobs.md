---
type: Rust Function
title: run_pst_jobs
resource: crates/lpe-admin-api/src/console.rs#L735-L746
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs
---

# Signature

`pub(crate) async fn run_pst_jobs( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<PstJobExecutionSummary>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [process_pending_pst_jobs](../../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)
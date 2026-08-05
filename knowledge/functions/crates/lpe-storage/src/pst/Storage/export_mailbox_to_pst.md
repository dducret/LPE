---
type: Rust Method
title: export_mailbox_to_pst
resource: crates/lpe-storage/src/pst.rs#L214-L313
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/pst/ensure_parent_directory
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs
  - functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup
---

# Signature

`async fn export_mailbox_to_pst(&self, job: &PendingPstJobRow) -> Result<u32>`

# Calls

- [ensure_parent_directory](../../../../../../functions/crates/lpe-storage/src/pst/ensure_parent_directory.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)

# Called by

- [process_pending_pst_jobs](../../../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)
- [pst_export_reconstructs_attachment_after_old_placement_cleanup](../../../../../../functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup.md)
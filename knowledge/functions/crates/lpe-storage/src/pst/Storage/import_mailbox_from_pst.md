---
type: Rust Method
title: import_mailbox_from_pst
resource: crates/lpe-storage/src/pst.rs#L315-L381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/pst/validate_pst_import_path
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/pst/decode_pst_field
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs
---

# Signature

`async fn import_mailbox_from_pst(&self, job: &PendingPstJobRow) -> Result<u32>`

# Calls

- [validate_pst_import_path](../../../../../../functions/crates/lpe-storage/src/pst/validate_pst_import_path.md)
- [open](../../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [decode_pst_field](../../../../../../functions/crates/lpe-storage/src/pst/decode_pst_field.md)
- [context](../../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)

# Called by

- [process_pending_pst_jobs](../../../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)
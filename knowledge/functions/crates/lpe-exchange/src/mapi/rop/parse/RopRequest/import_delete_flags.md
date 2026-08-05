---
type: Rust Method
title: import_delete_flags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L442-L444
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_hard_delete
---

# Signature

`pub(in crate::mapi) fn import_delete_flags(&self) -> u8`

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [import_delete_hard_delete](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_hard_delete.md)
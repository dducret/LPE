---
type: Rust Method
title: import_delete_hard_delete
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L446-L448
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
---

# Signature

`pub(in crate::mapi) fn import_delete_hard_delete(&self) -> bool`

# Calls

- [import_delete_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_flags.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
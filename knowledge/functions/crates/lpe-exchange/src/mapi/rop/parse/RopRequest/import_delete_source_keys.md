---
type: Rust Method
title: import_delete_source_keys
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L419-L440
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids
---

# Signature

`pub(in crate::mapi) fn import_delete_source_keys(&self) -> Vec<Vec<u8>>`

# Calls

- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [parse_tagged_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [import_delete_message_ids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids.md)
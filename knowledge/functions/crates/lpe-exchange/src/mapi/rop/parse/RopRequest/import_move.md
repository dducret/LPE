---
type: Rust Method
title: import_move
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L473-L505
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/read_nonempty_u32_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_message_move_decodes_length_prefixed_gids
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn import_move(&self) -> Option<ImportMessageMove<'_>>`

# Calls

- [read_nonempty_u32_prefixed_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/read_nonempty_u32_prefixed_bytes.md)
- [remaining](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)

# Called by

- [append_synchronization_import_message_move_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [outlook_sync_import_message_move_decodes_length_prefixed_gids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_message_move_decodes_length_prefixed_gids.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
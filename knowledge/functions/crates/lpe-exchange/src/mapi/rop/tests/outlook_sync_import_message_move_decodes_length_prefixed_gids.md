---
type: Rust Function
title: outlook_sync_import_message_move_decodes_length_prefixed_gids
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L5597-L5634
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`pub(in crate::mapi) fn outlook_sync_import_message_move_decodes_length_prefixed_gids()`

# Calls

- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [import_move](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
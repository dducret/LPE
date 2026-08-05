---
type: Rust Function
title: append_id_from_long_term_id_response
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L153-L201
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_object_id_conversion_response
---

# Signature

`pub(super) fn append_id_from_long_term_id_response( principal: &AccountPrincipal, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [long_term_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id.md)
- [debug_object_scope_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [rop_id_from_long_term_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response.md)

# Called by

- [append_object_id_conversion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_object_id_conversion_response.md)
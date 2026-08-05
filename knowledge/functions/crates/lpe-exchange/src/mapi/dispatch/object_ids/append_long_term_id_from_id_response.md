---
type: Rust Function
title: append_long_term_id_from_id_response
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L107-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_id_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_object_id_conversion_response
---

# Signature

`pub(super) fn append_long_term_id_from_id_response( principal: &AccountPrincipal, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [long_term_source_id_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_id_bytes.md)
- [long_term_source_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id.md)
- [debug_object_scope_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [rop_long_term_id_from_id_response_for_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope.md)

# Called by

- [append_object_id_conversion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_object_id_conversion_response.md)
---
type: Rust Function
title: rop_long_term_id_from_id_response_for_scope
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L91-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/long_term_id_from_id_object_is_loaded
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_long_term_id_from_id_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response
---

# Signature

`pub(super) fn rop_long_term_id_from_id_response_for_scope( request: &RopRequest, object_id: Option<u64>, scope: &str, ) -> Vec<u8>`

# Calls

- [long_term_id_from_id_object_is_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/long_term_id_from_id_object_is_loaded.md)
- [rop_long_term_id_from_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_long_term_id_from_id_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_long_term_id_from_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response.md)
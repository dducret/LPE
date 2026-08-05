---
type: Rust Function
title: rop_long_term_id_from_id_response
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L9-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope
  - functions/crates/lpe-exchange/src/mapi/rop/tests/long_term_id_from_id_accepts_outlook_and_emitted_counter_forms
---

# Signature

`pub(in crate::mapi) fn rop_long_term_id_from_id_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [long_term_source_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [rop_long_term_id_from_id_response_for_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope.md)
- [long_term_id_from_id_accepts_outlook_and_emitted_counter_forms](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/long_term_id_from_id_accepts_outlook_and_emitted_counter_forms.md)
---
type: Rust Function
title: unknown_property_wire_type_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L488-L514
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_have_known_wire_types
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn unknown_property_wire_type_response( principal: &AccountPrincipal, request: &RopRequest, ) -> Option<Vec<u8>>`

# Calls

- [property_tags_have_known_wire_types](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_have_known_wire_types.md)
- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
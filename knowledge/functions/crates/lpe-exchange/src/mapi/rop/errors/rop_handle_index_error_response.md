---
type: Rust Function
title: rop_handle_index_error_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L82-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(in crate::mapi) fn rop_handle_index_error_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_modify_permissions_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
---
type: Rust Function
title: append_modify_rules_response
resource: crates/lpe-exchange/src/mapi/dispatch/rules.rs#L46-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/rule_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_mutation_from_row
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_rules_dispatch_response
---

# Signature

`pub(super) async fn append_modify_rules_response<S>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_handle_index_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [modify_rules_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rules](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/rules.md)
- [rule_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/rule_audit.md)
- [bounded_rule_mutation_from_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_mutation_from_row.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)

# Called by

- [append_rules_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_rules_dispatch_response.md)
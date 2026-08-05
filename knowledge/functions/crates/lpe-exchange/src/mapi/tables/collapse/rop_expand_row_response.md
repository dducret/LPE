---
type: Rust Function
title: rop_expand_row_response
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L5-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/category_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/expand_max_row_count
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/expand_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_expand_row_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [category_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/category_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [expand_max_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/expand_max_row_count.md)
- [rop_expand_row_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response.md)

# Called by

- [expand_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/expand_row_response.md)
- [categorized_table_expand_collapse_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns.md)
- [microsoft_categorized_expand_collapse_report_current_state_errors](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors.md)
- [microsoft_contents_table_query_find_and_expand_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns.md)
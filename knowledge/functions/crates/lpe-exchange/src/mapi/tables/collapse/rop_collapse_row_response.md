---
type: Rust Function
title: rop_collapse_row_response
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L72-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/category_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_collapse_row_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/collapse_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors
---

# Signature

`pub(in crate::mapi) fn rop_collapse_row_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [category_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/category_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [rop_collapse_row_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_collapse_row_success_response.md)

# Called by

- [collapse_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/collapse_row_response.md)
- [categorized_table_expand_collapse_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns.md)
- [microsoft_categorized_expand_collapse_report_current_state_errors](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors.md)
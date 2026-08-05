---
type: Rust Function
title: apply_outlook_smart_input_variant_before_query_rows
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L3-L43
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/smart_input_variant_resets_inbox_fai_cursor_before_query_rows
---

# Signature

`pub(super) fn apply_outlook_smart_input_variant_before_query_rows( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, ) -> Option<String>`

# Calls

- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [smart_input_variant_resets_inbox_fai_cursor_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/smart_input_variant_resets_inbox_fai_cursor_before_query_rows.md)
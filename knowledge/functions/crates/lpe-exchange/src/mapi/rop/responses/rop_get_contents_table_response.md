---
type: Rust Function
title: rop_get_contents_table_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L148-L156
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_contents_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop
---

# Signature

`pub(in crate::mapi) fn rop_get_contents_table_response( request: &RopRequest, row_count: u32, ) -> Vec<u8>`

# Called by

- [get_contents_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_contents_table_response.md)
- [execute_rop_response_summary_keeps_contents_table_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary.md)
- [execute_rop_response_summary_skips_implausible_query_rows_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker.md)
- [execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop.md)
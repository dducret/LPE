---
type: Rust Function
title: rop_sort_table_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L322-L327
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker
---

# Signature

`pub(in crate::mapi) fn rop_sort_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/sort_table_response.md)
- [execute_rop_response_summary_keeps_contents_table_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary.md)
- [execute_rop_response_summary_skips_implausible_query_rows_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker.md)
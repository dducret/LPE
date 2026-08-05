---
type: Rust Function
title: rop_set_columns_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L315-L320
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop
  - functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_logon_example
---

# Signature

`pub(in crate::mapi) fn rop_set_columns_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_columns_response.md)
- [execute_rop_response_summary_keeps_contents_table_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary.md)
- [execute_rop_response_summary_skips_implausible_query_rows_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker.md)
- [execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop.md)
- [backoff_response_matches_microsoft_logon_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_logon_example.md)
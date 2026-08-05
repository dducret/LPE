---
type: Rust Function
title: rop_open_message_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L33-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_does_not_advertise_missing_recipient_rows
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_reload_cached_information_matches_open_message_shape
---

# Signature

`pub(in crate::mapi) fn rop_open_message_response( request: &RopRequest, subject: &str, recipient_count: usize, ) -> Vec<u8>`

# Calls

- [rop_open_message_response_with_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)
- [open_message_response_does_not_advertise_missing_recipient_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_does_not_advertise_missing_recipient_rows.md)
- [microsoft_reload_cached_information_matches_open_message_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_reload_cached_information_matches_open_message_shape.md)
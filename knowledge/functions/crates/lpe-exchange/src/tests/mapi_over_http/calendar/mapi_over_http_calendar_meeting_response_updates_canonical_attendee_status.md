---
type: Rust Function
title: mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L4266-L4372
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_open_message_with_flags
  - functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/save_staged_calendar_event
---

# Signature

`async fn mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status()`

# Calls

- [durable_special_folder_id_for_test](../../../../../../../functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_open_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_message_with_flags.md)
- [test_mapi_uuid_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [response_rops_and_handles_from_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body.md)
- [save_staged_calendar_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/save_staged_calendar_event.md)
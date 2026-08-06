---
type: Rust Function
title: strict_decode_content_sync_stream
resource: crates/lpe-exchange/src/tests/mod.rs#L13894-L14212
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/read_strict_u32
  - functions/crates/lpe-exchange/src/tests/strict_content_marker
  - functions/crates/lpe-exchange/src/tests/strict_finish_content_message
  - functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property
  - functions/crates/lpe-exchange/src/tests/strict_record_content_header_property
  - functions/crates/lpe-exchange/src/tests/strict_record_content_body_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_i32_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_utf16z
  - functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset
  - functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset
  - functions/crates/lpe-exchange/src/tests/strict_validate_store_xid
  - functions/crates/lpe-exchange/src/tests/strict_validate_change_key_xid
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number
---

# Signature

`fn strict_decode_content_sync_stream(bytes: &[u8]) -> Result<StrictContentSyncStream, String>`

# Calls

- [read_strict_u32](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_u32.md)
- [strict_content_marker](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_marker.md)
- [strict_finish_content_message](../../../../../functions/crates/lpe-exchange/src/tests/strict_finish_content_message.md)
- [strict_parse_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property.md)
- [strict_record_content_header_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_header_property.md)
- [strict_record_content_body_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_body_property.md)
- [strict_decode_i32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_i32_property.md)
- [strict_decode_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_utf16z.md)
- [strict_validate_replguid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset.md)
- [strict_validate_replid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset.md)
- [strict_validate_store_xid](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_store_xid.md)
- [strict_validate_change_key_xid](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_change_key_xid.md)
- [strict_replguid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter.md)
- [global_counter_from_globcnt](../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding.md)
- [strict_content_sync_transfer_from_response](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [strict_content_decoder_accepts_imported_change_key_with_server_change_number](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number.md)
---
type: Rust Function
title: strict_validate_replguid_globset
resource: crates/lpe-exchange/src/tests/mod.rs#L13475-L13478
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_validate_replguid_globset(value: &[u8]) -> Result<(), String>`

# Calls

- [strict_replguid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges.md)

# Called by

- [mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding.md)
- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)
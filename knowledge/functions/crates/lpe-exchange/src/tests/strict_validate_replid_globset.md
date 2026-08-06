---
type: Rust Function
title: strict_validate_replid_globset
resource: crates/lpe-exchange/src/tests/mod.rs#L14499-L14502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_validate_replid_globset(value: &[u8]) -> Result<(), String>`

# Calls

- [strict_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges.md)

# Called by

- [mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets.md)
- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)
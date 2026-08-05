---
type: Rust Function
title: strict_replguid_globset_contains_counter
resource: crates/lpe-exchange/src/tests/mod.rs#L13464-L13469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_replguid_globset_contains_counter(value: &[u8], counter: &[u8]) -> Result<bool, String>`

# Calls

- [strict_globcnt_to_u64](../../../../../functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64.md)
- [strict_replguid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges.md)

# Called by

- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)
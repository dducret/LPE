---
type: Rust Function
title: globcnt_slice_to_u64
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1272-L1274
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges
---

# Signature

`pub(super) fn globcnt_slice_to_u64(bytes: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [decode_globset_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges.md)
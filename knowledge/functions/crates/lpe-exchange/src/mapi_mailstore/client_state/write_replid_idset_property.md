---
type: Rust Function
title: write_replid_idset_property
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1108-L1116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_deletion_section
---

# Signature

`fn write_replid_idset_property(output: &mut Vec<u8>, property_tag: u32, counters: &CounterSet)`

# Calls

- [write_globset_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges.md)
- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [write_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_deletion_section.md)
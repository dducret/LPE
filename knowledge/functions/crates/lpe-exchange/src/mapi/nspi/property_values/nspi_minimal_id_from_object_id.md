---
type: Rust Function
title: nspi_minimal_id_from_object_id
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L662-L674
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
---

# Signature

`pub(in crate::mapi) fn nspi_minimal_id_from_object_id( object_id: u64, entry_kind: ExchangeAddressBookEntryKind, ) -> Option<u32>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
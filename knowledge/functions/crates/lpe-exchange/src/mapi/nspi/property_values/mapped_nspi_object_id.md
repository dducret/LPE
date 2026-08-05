---
type: Rust Function
title: mapped_nspi_object_id
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L627-L638
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_kind_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
---

# Signature

`fn mapped_nspi_object_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> Option<u64>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [nspi_identity_kind_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_kind_key.md)

# Called by

- [nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
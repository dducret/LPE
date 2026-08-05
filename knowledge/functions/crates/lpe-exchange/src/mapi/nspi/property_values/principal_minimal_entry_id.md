---
type: Rust Function
title: principal_minimal_entry_id
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L688-L693
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response
---

# Signature

`pub(in crate::mapi) fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32`

# Calls

- [nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)

# Called by

- [nspi_dn_to_mid_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)
- [nspi_minimal_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
---
type: Rust Function
title: nspi_entry_is_principal
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1136-L1141
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(in crate::mapi) fn nspi_entry_is_principal( entry: &ExchangeAddressBookEntry, principal: &AccountPrincipal, ) -> bool`

# Called by

- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
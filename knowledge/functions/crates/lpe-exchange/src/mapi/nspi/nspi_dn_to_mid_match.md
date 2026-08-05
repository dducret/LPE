---
type: Rust Function
title: nspi_dn_to_mid_match
resource: crates/lpe-exchange/src/mapi/nspi.rs#L544-L571
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/principal_minimal_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
---

# Signature

`fn nspi_dn_to_mid_match( principal: &AccountPrincipal, entries: &[ExchangeAddressBookEntry], values: &[String], ) -> NspiDnToMidMatch`

# Calls

- [nspi_match_dn_to_mid_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [nspi_lookup_matches_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)
- [principal_minimal_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/principal_minimal_entry_id.md)

# Called by

- [nspi_dn_to_mid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
---
type: Rust Function
title: allocate_nspi_entry_identities
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L549-L562
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
---

# Signature

`pub(in crate::mapi) async fn allocate_nspi_entry_identities<S>( store: &S, principal: &AccountPrincipal, entries: &[ExchangeAddressBookEntry], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [remember_nspi_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records.md)

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_dn_to_mid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
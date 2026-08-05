---
type: Rust Function
title: allocate_principal_nspi_identity
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L564-L576
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_request
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response
---

# Signature

`pub(in crate::mapi) async fn allocate_principal_nspi_identity<S>( store: &S, principal: &AccountPrincipal, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [nspi_identity_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_request.md)
- [remember_nspi_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity_records.md)

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_dn_to_mid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_minimal_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
- [nspi_template_info_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
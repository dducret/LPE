---
type: Rust Function
title: apply_canonical_contact_property_values
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L680-L706
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/contact/reject_unsupported_mapi_contact_properties
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_contact_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, contact_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [reject_unsupported_mapi_contact_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/reject_unsupported_mapi_contact_properties.md)
- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
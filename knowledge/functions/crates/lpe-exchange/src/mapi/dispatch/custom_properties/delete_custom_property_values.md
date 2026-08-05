---
type: Rust Function
title: delete_custom_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L292-L321
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(super) async fn delete_custom_property_values<S>( store: &S, principal: &AccountPrincipal, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [custom_property_storage_account_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id.md)
- [delete_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_custom_property_values.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
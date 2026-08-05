---
type: Rust Function
title: fetch_custom_property_values_for_request
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L125-L170
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(super) async fn fetch_custom_property_values_for_request<S>( store: &S, principal: &AccountPrincipal, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Result<HashMap<u32, Vec<u8>>> where S: ExchangeStore,`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [custom_property_storage_account_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id.md)
- [fetch_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
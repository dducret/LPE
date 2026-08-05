---
type: Rust Function
title: copy_custom_property_values_for_request
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L172-L235
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
---

# Signature

`pub(super) async fn copy_custom_property_values_for_request<S>( store: &S, principal: &AccountPrincipal, source: Option<&MapiObject>, destination: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Result<Option<Vec<(usize, u32, u32)>>> where S: ExchangeStore,`

# Calls

- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [custom_property_storage_account_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id.md)
- [fetch_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values.md)
- [staged_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [upsert_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values.md)

# Called by

- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
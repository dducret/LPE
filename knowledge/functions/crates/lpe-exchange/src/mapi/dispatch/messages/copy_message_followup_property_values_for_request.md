---
type: Rust Function
title: copy_message_followup_property_values_for_request
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L537-L594
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
---

# Signature

`pub(super) async fn copy_message_followup_property_values_for_request<S>( store: &S, principal: &AccountPrincipal, source: Option<&MapiObject>, destination: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Result<Option<Vec<(usize, u32, u32)>>> where S: ExchangeStore,`

# Calls

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)

# Called by

- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
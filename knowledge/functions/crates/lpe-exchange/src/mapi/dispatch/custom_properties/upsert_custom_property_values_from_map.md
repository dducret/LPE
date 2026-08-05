---
type: Rust Function
title: upsert_custom_property_values_from_map
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L67-L83
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn upsert_custom_property_values_from_map<S>( store: &S, principal: &AccountPrincipal, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, properties: &HashMap<u32, MapiValue>, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [upsert_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values.md)

# Called by

- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
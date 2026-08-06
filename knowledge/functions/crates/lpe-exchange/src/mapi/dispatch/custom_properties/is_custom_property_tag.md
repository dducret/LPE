---
type: Rust Function
title: is_custom_property_tag
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L442-L447
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_canonical_named_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(super) fn is_custom_property_tag(property_tag: u32) -> bool`

# Calls

- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [is_canonical_named_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_canonical_named_property_tag.md)

# Called by

- [stage_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values.md)
- [stage_contact_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_deletions.md)
- [staged_contact_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input.md)
- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [upsert_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map.md)
- [mapi_event_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map.md)
- [mapi_contact_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map.md)
- [fetch_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request.md)
- [delete_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values.md)
- [staged_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
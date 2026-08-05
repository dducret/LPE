---
type: Rust Function
title: size_limited_specific_properties
resource: crates/lpe-exchange/src/mapi/rop/property_limits.rs#L4-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/request_property_size_limit
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/flagged_property_cell_size
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/specific_property_supports_stream
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/aggregate_response_budget_limits_streamable_values_by_occurrence
---

# Signature

`pub(super) fn size_limited_specific_properties( request: &RopRequest, object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, columns: &[u32], unsupported_tags: &[u32], custom_values: &HashMap<u32, Vec<u8>>, response_size_limit: usize, ) -> Vec<bool>`

# Calls

- [request_property_size_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/request_property_size_limit.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [get_properties_specific_value_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)
- [get_properties_specific_typed_value_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)
- [flagged_property_cell_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/flagged_property_cell_size.md)
- [specific_property_supports_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/specific_property_supports_stream.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [aggregate_response_budget_limits_streamable_values_by_occurrence](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/aggregate_response_budget_limits_streamable_values_by_occurrence.md)
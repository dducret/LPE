---
type: Rust Method
title: property_id
resource: crates/lpe-exchange/src/mapi/properties/tags.rs#L13-L15
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
  - functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_modeled_empty_property
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags
---

# Signature

`pub(in crate::mapi) fn property_id(self) -> u16`

# Called by

- [is_custom_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [log_get_properties_specific_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)
- [record_outlook_umolk_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)
- [normal_message_defaulted_column_detail](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail.md)
- [normalize_table_property_tag_for_session](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session.md)
- [table_column_support_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary.md)
- [contact_property_value_with_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [fast_transfer_named_property_for_message_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag.md)
- [well_known_named_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_properties.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [normalize_named_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)
- [populate_special_message_named_property_definitions](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)
- [associated_config_modeled_empty_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_modeled_empty_property.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [associated_config_named_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags.md)
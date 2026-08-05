---
type: Rust Function
title: associated_config_property_value
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L453-L458
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_open_shape
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_binary_property_len
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags
---

# Signature

`pub(in crate::mapi) fn associated_config_property_value( message: &MapiAssociatedConfigMessage, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)

# Called by

- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [associated_config_open_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_open_shape.md)
- [associated_config_binary_property_len](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_binary_property_len.md)
- [format_ipm_configuration_row_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract.md)
- [ipm_configuration_row_issues](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues.md)
- [restriction_matches_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [format_associated_config_0e0b_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug.md)
- [format_ipm_configuration_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)
- [associated_config_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [associated_config_named_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags.md)
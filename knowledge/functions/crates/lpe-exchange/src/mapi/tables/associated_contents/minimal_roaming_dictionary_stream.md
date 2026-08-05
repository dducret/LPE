---
type: Rust Function
title: minimal_roaming_dictionary_stream
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L991-L993
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
---

# Signature

`pub(in crate::mapi) fn minimal_roaming_dictionary_stream() -> Vec<u8>`

# Called by

- [normalized_associated_config_persisted_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties.md)
- [sanitize_configuration_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
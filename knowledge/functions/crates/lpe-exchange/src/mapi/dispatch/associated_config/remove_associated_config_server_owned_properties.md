---
type: Rust Function
title: remove_associated_config_server_owned_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L280-L283
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_server_owned_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties
---

# Signature

`fn remove_associated_config_server_owned_properties(properties: &mut HashMap<u32, MapiValue>)`

# Calls

- [is_associated_config_server_owned_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_server_owned_property_tag.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [normalized_associated_config_content_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties.md)
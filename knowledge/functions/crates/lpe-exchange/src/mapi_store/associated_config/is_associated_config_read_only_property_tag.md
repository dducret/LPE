---
type: Rust Function
title: is_associated_config_read_only_property_tag
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L33-L39
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_server_owned_property_tag
---

# Signature

`pub(crate) fn is_associated_config_read_only_property_tag(property_tag: u32) -> bool`

# Called by

- [set_associated_config_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [apply_pending_associated_message_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values.md)
- [sync_stream_target](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)
- [associated_config_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [is_associated_config_server_owned_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_server_owned_property_tag.md)
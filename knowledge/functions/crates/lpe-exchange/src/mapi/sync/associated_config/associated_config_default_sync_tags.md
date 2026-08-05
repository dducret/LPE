---
type: Rust Function
title: associated_config_default_sync_tags
resource: crates/lpe-exchange/src/mapi/sync/associated_config.rs#L99-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
---

# Signature

`fn associated_config_default_sync_tags( message: &crate::mapi_store::MapiAssociatedConfigMessage, ) -> &'static [u32]`

# Calls

- [is_outlook_configuration_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)

# Called by

- [associated_config_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
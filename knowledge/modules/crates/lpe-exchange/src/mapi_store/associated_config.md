---
type: Rust Module
title: associated_config
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L1-L565
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-storage-delegatefreebusymessageobject
  - external/uuid-uuid
  - external/super-mapiassociatedconfigmessage-mapidelegatefreebusymessage
  - external/crate-store-mapiassociatedconfigrecord-upsertmapiassociatedconfiginput
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [copy_associated_config_server_metadata](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata.md)
- [is_associated_config_read_only_property_tag](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)
- [is_associated_config_server_owned_property_tag](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_server_owned_property_tag.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)
- [is_outlook_inbox_virtual_only_associated_config_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id.md)
- [outlook_default_folder_named_view_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)
- [outlook_default_folder_named_view_name](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)
- [is_outlook_default_conversation_action_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [is_outlook_local_freebusy_message_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)
- [outlook_inbox_associated_config_defaults](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults.md)
- [outlook_inbox_associated_config_sync_defaults](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults.md)
- [outlook_inbox_exact_virtual_associated_config_for_message_class](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class.md)
- [outlook_inbox_exact_virtual_associated_config_for_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id.md)
- [modeled_virtual_associated_config_message_for_canonical_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id.md)
- [format_associated_config_classes](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/format_associated_config_classes.md)
- [format_associated_config_inputs](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/format_associated_config_inputs.md)
- [log_outlook_inbox_associated_config_bootstrap](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/log_outlook_inbox_associated_config_bootstrap.md)
- [is_empty_synthetic_inbox_associated_config](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_synthetic_inbox_associated_config.md)
- [is_empty_outlook_rule_organizer_placeholder](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_outlook_rule_organizer_placeholder.md)
- [is_stale_outlook_umolk_user_options_placeholder](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_stale_outlook_umolk_user_options_placeholder.md)
- [is_outlook_configuration_message_class](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [is_outlook_configuration_message_class_name](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name.md)
- [is_outlook_umolk_user_options_message_class](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class.md)
- [virtual_local_freebusy_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/virtual_local_freebusy_message.md)
- [ensure_virtual_local_freebusy_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/ensure_virtual_local_freebusy_message.md)

# Imports

- `lpe_storage::DelegateFreeBusyMessageObject`
- `uuid::Uuid`
- `super::{MapiAssociatedConfigMessage, MapiDelegateFreeBusyMessage}`
- `crate::store::{MapiAssociatedConfigRecord, UpsertMapiAssociatedConfigInput}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
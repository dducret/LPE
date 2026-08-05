---
type: Rust Function
title: is_expected_unbacked_mapi_object
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1259-L1265
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_identity_resolution
---

# Signature

`fn is_expected_unbacked_mapi_object(object_id: u64) -> bool`

# Calls

- [is_advertised_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)
- [is_outlook_default_conversation_action_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [is_outlook_local_freebusy_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)

# Called by

- [log_mapi_requested_identity_resolution](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_identity_resolution.md)
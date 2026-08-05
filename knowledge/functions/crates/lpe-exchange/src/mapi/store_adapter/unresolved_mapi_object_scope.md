---
type: Rust Function
title: unresolved_mapi_object_scope
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1236-L1257
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
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
---

# Signature

`fn unresolved_mapi_object_scope(object_id: u64) -> &'static str`

# Calls

- [is_advertised_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)
- [is_outlook_default_conversation_action_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [is_outlook_local_freebusy_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)
- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
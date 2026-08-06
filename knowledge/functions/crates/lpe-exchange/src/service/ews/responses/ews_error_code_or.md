---
type: Rust Function
title: ews_error_code_or
resource: crates/lpe-exchange/src/service/ews/responses.rs#L94-L105
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
  - functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/refresh_sharing_folder
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration
---

# Signature

`pub(in crate::service) fn ews_error_code_or( error: &anyhow::Error, fallback: &'static str, ) -> &'static str`

# Called by

- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [find_conversation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation.md)
- [get_conversation_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items.md)
- [apply_conversation_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action.md)
- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)
- [mutate_ews_delegates](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)
- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
- [create_managed_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [archive_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item.md)
- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
- [delete_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [mark_as_junk](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk.md)
- [get_sharing_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder.md)
- [refresh_sharing_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/refresh_sharing_folder.md)
- [play_on_phone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone.md)
- [create_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration.md)
- [update_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration.md)
- [delete_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration.md)
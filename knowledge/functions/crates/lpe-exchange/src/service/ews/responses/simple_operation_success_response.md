---
type: Rust Function
title: simple_operation_success_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L207-L220
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/mark_all_items_as_read
  - functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action
  - functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules
  - functions/crates/lpe-exchange/src/service/ews/ucs/simple_ews_operation_result
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration
---

# Signature

`pub(in crate::service) fn simple_operation_success_response(operation: &str) -> String`

# Called by

- [empty_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder.md)
- [apply_conversation_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action.md)
- [send_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item.md)
- [mark_all_items_as_read](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/mark_all_items_as_read.md)
- [perform_reminder_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action.md)
- [update_inbox_rules](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules.md)
- [simple_ews_operation_result](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/simple_ews_operation_result.md)
- [create_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration.md)
- [update_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration.md)
- [delete_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration.md)
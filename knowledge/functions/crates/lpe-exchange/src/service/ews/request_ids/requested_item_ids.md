---
type: Rust Function
title: requested_item_ids
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L3-L8
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
  - functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk
  - functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_transfer_item_ids
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone
---

# Signature

`pub(in crate::service) fn requested_item_ids(request: &str) -> Vec<String>`

# Calls

- [requested_item_references](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references.md)

# Called by

- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [export_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items.md)
- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [send_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item.md)
- [archive_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item.md)
- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
- [delete_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [mark_as_junk](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk.md)
- [perform_reminder_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action.md)
- [requested_transfer_item_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_transfer_item_ids.md)
- [play_on_phone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone.md)
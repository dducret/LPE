---
type: Rust Function
title: message_item_xml
resource: crates/lpe-exchange/src/service/ews/mail.rs#L50-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
---

# Signature

`pub(in crate::service) fn message_item_xml(email: &JmapEmail) -> String`

# Calls

- [message_item_xml_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_attachments.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [archive_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item.md)
- [copy_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
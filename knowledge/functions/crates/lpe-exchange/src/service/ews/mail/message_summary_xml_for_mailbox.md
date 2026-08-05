---
type: Rust Function
title: message_summary_xml_for_mailbox
resource: crates/lpe-exchange/src/service/ews/mail.rs#L17-L48
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn message_summary_xml_for_mailbox( email: &JmapEmail, mailbox_id: Uuid, ) -> String`

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [message_summary_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)
---
type: Rust Function
title: message_item_xml_with_attachments
resource: crates/lpe-exchange/src/service/ews/mail.rs#L54-L59
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml
---

# Signature

`fn message_item_xml_with_attachments( email: &JmapEmail, attachments: &[ActiveSyncAttachment], ) -> String`

# Calls

- [message_item_xml_with_details](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details.md)

# Called by

- [message_item_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml.md)
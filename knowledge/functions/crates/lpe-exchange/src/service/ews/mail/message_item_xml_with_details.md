---
type: Rust Function
title: message_item_xml_with_details
resource: crates/lpe-exchange/src/service/ews/mail.rs#L61-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_attachments
---

# Signature

`pub(in crate::service) fn message_item_xml_with_details( email: &JmapEmail, attachments: &[ActiveSyncAttachment], mime_attachment_contents: Option<&[ActiveSyncAttachmentContent]>, ) -> String`

# Calls

- [message_summary_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml.md)

# Called by

- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [message_item_xml_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_attachments.md)
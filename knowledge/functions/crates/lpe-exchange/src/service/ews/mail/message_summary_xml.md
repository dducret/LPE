---
type: Rust Function
title: message_summary_xml
resource: crates/lpe-exchange/src/service/ews/mail.rs#L13-L15
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details
---

# Signature

`pub(in crate::service) fn message_summary_xml(email: &JmapEmail) -> String`

# Calls

- [message_summary_xml_for_mailbox](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml_for_mailbox.md)

# Called by

- [message_item_xml_with_details](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details.md)
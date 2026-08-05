---
type: Rust Function
title: root_item_id_xml
resource: crates/lpe-exchange/src/service/ews/mail.rs#L87-L93
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/delete_attachment
---

# Signature

`pub(in crate::service) fn root_item_id_xml(email: &JmapEmail) -> String`

# Called by

- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [delete_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/delete_attachment.md)
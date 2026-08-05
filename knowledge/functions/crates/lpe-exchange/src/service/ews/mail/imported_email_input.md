---
type: Rust Function
title: imported_email_input
resource: crates/lpe-exchange/src/service/ews/mail.rs#L262-L291
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn imported_email_input( input: SubmitMessageInput, mailbox_id: Uuid, ) -> JmapImportedEmailInput`

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
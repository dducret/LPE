---
type: Rust Function
title: embedded_message_properties_from_attachment_metadata
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1347-L1361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment
---

# Signature

`async fn embedded_message_properties_from_attachment_metadata<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, file_reference: &str, file_name: &str, ) -> HashMap<u32, MapiValue>`

# Calls

- [fetch_attachment_content](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)

# Called by

- [open_embedded_message_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source.md)
- [embedded_message_properties_from_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment.md)
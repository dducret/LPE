---
type: Rust Function
title: embedded_message_properties_from_attachment
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1333-L1345
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata
---

# Signature

`async fn embedded_message_properties_from_attachment<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, attachment: &crate::mapi_store::MapiAttachment, ) -> HashMap<u32, MapiValue>`

# Calls

- [embedded_message_properties_from_attachment_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_attachment_metadata.md)
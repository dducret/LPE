---
type: Rust Function
title: mailbox_property_value_with_context
resource: crates/lpe-exchange/src/mapi/properties.rs#L490-L496
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
---

# Signature

`pub(in crate::mapi) fn mailbox_property_value_with_context( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
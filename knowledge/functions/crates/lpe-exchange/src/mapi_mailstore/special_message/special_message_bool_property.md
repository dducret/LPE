---
type: Rust Function
title: special_message_bool_property
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L65-L76
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags
---

# Signature

`fn special_message_bool_property( object: &SpecialMessageSyncFact, property_tag: u32, ) -> Option<bool>`

# Called by

- [special_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags.md)
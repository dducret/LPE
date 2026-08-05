---
type: Rust Function
title: parse_xml_bool
resource: crates/lpe-exchange/src/service/ews/xml.rs#L302-L308
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags
---

# Signature

`pub(in crate::service) fn parse_xml_bool(value: &str) -> Result<bool>`

# Called by

- [parse_conversation_actions](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions.md)
- [parse_ews_delegate_user](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)
- [parse_update_message_flags](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags.md)
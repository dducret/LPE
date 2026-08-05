---
type: Rust Function
title: parse_conversation_actions
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L374-L391
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action
---

# Signature

`pub(in crate::service) fn parse_conversation_actions( request: &str, ) -> Vec<ConversationActionRequest>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [requested_mailbox_folder_ids_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in.md)
- [parse_xml_bool](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool.md)

# Called by

- [apply_conversation_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/apply_conversation_action.md)
---
type: Rust Function
title: parse_first_mailbox
resource: crates/lpe-exchange/src/service/ews/mailboxes.rs#L27-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response
---

# Signature

`pub(in crate::service) fn parse_first_mailbox(value: &str) -> Option<ParsedMailbox>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [expand_dl_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response.md)
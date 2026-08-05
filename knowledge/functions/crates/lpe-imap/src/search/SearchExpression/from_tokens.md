---
type: Rust Method
title: from_tokens
resource: crates/lpe-imap/src/search.rs#L39-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/search/parse_search_key
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_search
  - functions/crates/lpe-imap/src/search/parse_search_key
---

# Signature

`pub(crate) fn from_tokens(tokens: &[String]) -> Result<Self>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_search_key](../../../../../../functions/crates/lpe-imap/src/search/parse_search_key.md)

# Called by

- [handle_search](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_search.md)
- [parse_search_key](../../../../../../functions/crates/lpe-imap/src/search/parse_search_key.md)
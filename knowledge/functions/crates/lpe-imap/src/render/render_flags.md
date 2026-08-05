---
type: Rust Function
title: render_flags
resource: crates/lpe-imap/src/render.rs#L374-L397
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/render/imap_keyword_atom
---

# Signature

`pub(crate) fn render_flags(email: &ImapEmail, mailbox_name: &str) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [imap_keyword_atom](../../../../../functions/crates/lpe-imap/src/render/imap_keyword_atom.md)
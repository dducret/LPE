---
type: Rust Function
title: render_status_response
resource: crates/lpe-imap/src/render.rs#L409-L435
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_status
---

# Signature

`pub(crate) fn render_status_response( mailbox_name: &str, emails: &[ImapEmail], requested: &[String], state: &ImapMailboxState, utf8_enabled: bool, ) -> String`

# Called by

- [handle_status](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
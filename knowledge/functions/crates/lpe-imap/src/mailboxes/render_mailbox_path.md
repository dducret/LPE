---
type: Rust Function
title: render_mailbox_path
resource: crates/lpe-imap/src/mailboxes.rs#L739-L759
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_status
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path
  - functions/crates/lpe-imap/src/service/Session/handle_getquotaroot
---

# Signature

`pub(crate) fn render_mailbox_path(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_mailbox_listing](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing.md)
- [handle_lsub](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub.md)
- [handle_status](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
- [handle_select_mode](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode.md)
- [mailbox_matches_path](../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)
- [handle_getquotaroot](../../../../../functions/crates/lpe-imap/src/service/Session/handle_getquotaroot.md)
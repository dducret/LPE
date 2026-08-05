---
type: Rust Function
title: mailbox_name_matches
resource: crates/lpe-imap/src/render.rs#L42-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path
---

# Signature

`pub(crate) fn mailbox_name_matches(display_name: &str, role: &str, requested: &str) -> bool`

# Calls

- [system_role_for_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)
- [collides_with](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [mailbox_matches_path](../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)
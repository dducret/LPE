---
type: Rust Function
title: parse_mailbox_path
resource: crates/lpe-imap/src/parse.rs#L120-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_rename
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name
  - functions/crates/lpe-imap/src/mailboxes/parse_select_mailbox_path
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path_token
---

# Signature

`pub(crate) fn parse_mailbox_path(value: &str) -> Result<MailboxPath>`

# Calls

- [system_role_for_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)

# Called by

- [handle_rename](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_rename.md)
- [resolve_mailbox_name](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name.md)
- [parse_select_mailbox_path](../../../../../functions/crates/lpe-imap/src/mailboxes/parse_select_mailbox_path.md)
- [parse_mailbox_path_token](../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)
---
type: Rust Function
title: parse_mailbox_path_token
resource: crates/lpe-imap/src/parse.rs#L115-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/first_token
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_status
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_create
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
---

# Signature

`pub(crate) fn parse_mailbox_path_token(arguments: &str, error: &str) -> Result<MailboxPath>`

# Calls

- [first_token](../../../../../functions/crates/lpe-imap/src/parse/first_token.md)
- [parse_mailbox_path](../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)

# Called by

- [handle_status](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
- [handle_create](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_create.md)
- [resolve_mailbox_by_name](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
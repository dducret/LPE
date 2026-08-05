---
type: Rust Method
title: resolve_mailbox_by_name
resource: crates/lpe-imap/src/mailboxes.rs#L658-L661
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path_token
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/handle_getacl
  - functions/crates/lpe-imap/src/acl/Session/handle_myrights
  - functions/crates/lpe-imap/src/acl/Session/handle_listrights
  - functions/crates/lpe-imap/src/acl/Session/apply_acl_update
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_subscribe
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_unsubscribe
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_delete
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
  - functions/crates/lpe-imap/src/service/Session/handle_getquotaroot
---

# Signature

`pub(crate) async fn resolve_mailbox_by_name(&self, arguments: &str) -> Result<JmapMailbox>`

# Calls

- [parse_mailbox_path_token](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)
- [resolve_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path.md)

# Called by

- [handle_getacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_getacl.md)
- [handle_myrights](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_myrights.md)
- [handle_listrights](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_listrights.md)
- [apply_acl_update](../../../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)
- [handle_subscribe](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_subscribe.md)
- [handle_unsubscribe](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unsubscribe.md)
- [handle_delete](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_delete.md)
- [handle_copy](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [handle_getquotaroot](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_getquotaroot.md)
---
type: Rust Method
title: apply_acl_update
resource: crates/lpe-imap/src/acl.rs#L195-L334
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  - functions/crates/lpe-imap/src/acl/combine_acl_state
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-imap/src/acl/parse_acl_state_update
  - functions/crates/lpe-imap/src/acl/sync_sender_right
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/handle_setacl
  - functions/crates/lpe-imap/src/acl/Session/handle_deleteacl
---

# Signature

`async fn apply_acl_update( &mut self, mailbox_name: &str, identifier: &str, rights: &str, ) -> Result<()>`

# Calls

- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [combine_acl_state](../../../../../../functions/crates/lpe-imap/src/acl/combine_acl_state.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [parse_acl_state_update](../../../../../../functions/crates/lpe-imap/src/acl/parse_acl_state_update.md)
- [sync_sender_right](../../../../../../functions/crates/lpe-imap/src/acl/sync_sender_right.md)

# Called by

- [handle_setacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_setacl.md)
- [handle_deleteacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_deleteacl.md)
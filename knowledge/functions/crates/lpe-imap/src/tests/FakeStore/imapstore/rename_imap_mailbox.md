---
type: Rust Method
title: rename_imap_mailbox
resource: crates/lpe-imap/src/tests.rs#L491-L553
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments
  - functions/crates/lpe-imap/src/tests/mailbox_name_match
  - functions/crates/lpe-imap/src/tests/mailbox_name_collides
  - functions/crates/lpe-imap/src/tests/mailbox_with_parent
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/tests/mailbox_parent_creates_cycle
---

# Signature

`fn rename_imap_mailbox<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, name: &'a str, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [segments](../../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments.md)
- [mailbox_name_match](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_name_match.md)
- [mailbox_name_collides](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_name_collides.md)
- [mailbox_with_parent](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_with_parent.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mailbox_parent_creates_cycle](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_parent_creates_cycle.md)
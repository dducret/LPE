---
type: Rust Method
title: create_imap_mailbox
resource: crates/lpe-imap/src/tests.rs#L421-L489
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/system_mailbox_for_name
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments
  - functions/crates/lpe-imap/src/tests/mailbox_name_match
  - functions/crates/lpe-imap/src/tests/mailbox_name_collides
  - functions/crates/lpe-imap/src/tests/mailbox_with_parent
---

# Signature

`fn create_imap_mailbox<'a>( &'a self, account_id: Uuid, name: &'a str, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [system_mailbox_for_name](../../../../../../../functions/crates/lpe-imap/src/tests/system_mailbox_for_name.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [segments](../../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments.md)
- [mailbox_name_match](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_name_match.md)
- [mailbox_name_collides](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_name_collides.md)
- [mailbox_with_parent](../../../../../../../functions/crates/lpe-imap/src/tests/mailbox_with_parent.md)
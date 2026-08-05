---
type: Rust Method
title: update_jmap_email_flags
resource: crates/lpe-activesync/src/store.rs#L444-L462
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail_items/update_message_flags
---

# Signature

`fn update_jmap_email_flags<'a>( &'a self, account_id: Uuid, message_id: Uuid, unread: Option<bool>, flagged: Option<bool>, audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`

# Calls

- [update_message_flags](../../../../../../../functions/crates/lpe-storage/src/mail_items/update_message_flags.md)
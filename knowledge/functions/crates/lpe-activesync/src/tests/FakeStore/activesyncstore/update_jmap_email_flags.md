---
type: Rust Method
title: update_jmap_email_flags
resource: crates/lpe-activesync/src/tests.rs#L691-L719
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_flags<'a>( &'a self, _account_id: Uuid, message_id: Uuid, unread: Option<bool>, flagged: Option<bool>, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
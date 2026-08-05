---
type: Rust Method
title: update_jmap_email_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L11518-L11539
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_flags<'a>( &'a self, _account_id: Uuid, message_id: Uuid, unread: Option<bool>, flagged: Option<bool>, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
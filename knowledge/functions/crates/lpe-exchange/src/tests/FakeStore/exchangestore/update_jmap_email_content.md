---
type: Rust Method
title: update_jmap_email_content
resource: crates/lpe-exchange/src/tests/mod.rs#L11663-L11685
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_content<'a>( &'a self, _account_id: Uuid, message_id: Uuid, subject: Option<String>, body_text: Option<String>, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
---
type: Rust Method
title: update_jmap_email_followup_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L11796-L11859
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_followup_flags<'a>( &'a self, _account_id: Uuid, message_id: Uuid, update: lpe_storage::JmapEmailFollowupUpdate, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
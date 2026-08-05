---
type: Rust Method
title: update_jmap_email_followup_flags
resource: crates/lpe-activesync/src/tests.rs#L721-L800
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_followup_flags<'a>( &'a self, _account_id: Uuid, message_id: Uuid, update: JmapEmailFollowupUpdate, _audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
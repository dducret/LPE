---
type: Rust Method
title: update_jmap_email_followup_flags
resource: crates/lpe-activesync/src/store.rs#L464-L475
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn update_jmap_email_followup_flags<'a>( &'a self, account_id: Uuid, message_id: Uuid, update: JmapEmailFollowupUpdate, audit: AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`
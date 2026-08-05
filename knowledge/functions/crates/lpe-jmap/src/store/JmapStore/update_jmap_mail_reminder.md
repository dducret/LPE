---
type: Rust Method
title: update_jmap_mail_reminder
resource: crates/lpe-jmap/src/store.rs#L454-L472
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn update_jmap_mail_reminder( &self, account_id: Uuid, message_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, audit: AuditEntryInput, ) -> Result<()>`
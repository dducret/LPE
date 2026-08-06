---
type: Rust Method
title: update_jmap_task_reminder
resource: crates/lpe-jmap/src/store.rs#L1176-L1194
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn update_jmap_task_reminder( &self, principal_account_id: Uuid, task_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, reminder_reset: Option<bool>, ) -> Result<()>`
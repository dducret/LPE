---
type: Rust Method
title: update_jmap_event_reminder
resource: crates/lpe-jmap/src/store.rs#L437-L453
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn update_jmap_event_reminder( &self, principal_account_id: Uuid, event_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, ) -> Result<()>`
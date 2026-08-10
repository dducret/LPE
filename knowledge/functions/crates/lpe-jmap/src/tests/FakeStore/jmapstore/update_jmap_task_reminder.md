---
type: Rust Method
title: update_jmap_task_reminder
resource: crates/lpe-jmap/src/tests.rs#L2232-L2253
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/update_fake_reminder
---

# Signature

`async fn update_jmap_task_reminder( &self, _principal_account_id: Uuid, task_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, _reminder_reset: Option<bool>, ) -> Result<()>`

# Calls

- [update_fake_reminder](../../../../../../../functions/crates/lpe-jmap/src/tests/update_fake_reminder.md)
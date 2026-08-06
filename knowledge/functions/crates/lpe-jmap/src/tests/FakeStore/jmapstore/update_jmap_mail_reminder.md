---
type: Rust Method
title: update_jmap_mail_reminder
resource: crates/lpe-jmap/src/tests.rs#L2278-L2304
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/update_fake_reminder
---

# Signature

`async fn update_jmap_mail_reminder( &self, _account_id: Uuid, message_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, _audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [update_fake_reminder](../../../../../../../functions/crates/lpe-jmap/src/tests/update_fake_reminder.md)
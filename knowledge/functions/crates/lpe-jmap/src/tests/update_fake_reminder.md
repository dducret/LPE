---
type: Rust Function
title: update_fake_reminder
resource: crates/lpe-jmap/src/tests.rs#L2306-L2344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_task_reminder
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_event_reminder
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_mail_reminder
---

# Signature

`fn update_fake_reminder( reminders: &Arc<Mutex<Vec<ClientReminder>>>, source_type: &str, source_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, ) -> Result<()>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [update_jmap_task_reminder](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_task_reminder.md)
- [update_jmap_event_reminder](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_event_reminder.md)
- [update_jmap_mail_reminder](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_mail_reminder.md)
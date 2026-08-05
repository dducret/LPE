---
type: Rust Method
title: update_jmap_email_followup_flags
resource: crates/lpe-admin-api/src/workspace/tests.rs#L130-L145
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-admin-api/src/workspace/tests/jmap_email
---

# Signature

`async fn update_jmap_email_followup_flags( &self, account_id: Uuid, message_id: Uuid, update: JmapEmailFollowupUpdate, audit: AuditEntryInput, ) -> anyhow::Result<JmapEmail>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [jmap_email](../../../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/jmap_email.md)
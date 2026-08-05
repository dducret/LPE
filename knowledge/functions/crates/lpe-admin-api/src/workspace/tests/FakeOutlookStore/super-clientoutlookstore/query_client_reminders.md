---
type: Rust Method
title: query_client_reminders
resource: crates/lpe-admin-api/src/workspace/tests.rs#L275-L285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn query_client_reminders( &self, account_id: Uuid, query: ReminderQuery, ) -> anyhow::Result<Vec<ClientReminder>>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
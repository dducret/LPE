---
type: Rust Method
title: update_accessible_event_reminder
resource: crates/lpe-exchange/src/tests/mod.rs#L8866-L8900
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn update_accessible_event_reminder<'a>( &'a self, _principal_account_id: Uuid, event_id: Uuid, reminder_set: Option<bool>, reminder_at: Option<String>, reminder_dismissed_at: Option<String>, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
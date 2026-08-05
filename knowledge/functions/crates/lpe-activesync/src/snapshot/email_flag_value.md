---
type: Rust Function
title: email_flag_value
resource: crates/lpe-activesync/src/snapshot.rs#L96-L132
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/message/activesync_timestamp
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/email_application_data
---

# Signature

`fn email_flag_value(email: &JmapEmail) -> Option<Value>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [activesync_timestamp](../../../../../functions/crates/lpe-activesync/src/message/activesync_timestamp.md)

# Called by

- [email_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/email_application_data.md)
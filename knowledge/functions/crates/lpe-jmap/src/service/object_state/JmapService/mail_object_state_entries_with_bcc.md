---
type: Rust Method
title: mail_object_state_entries_with_bcc
resource: crates/lpe-jmap/src/service/object_state.rs#L180-L236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/email_state_fingerprint
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`async fn mail_object_state_entries_with_bcc( &self, account_id: Uuid, data_type: &str, include_bcc: bool, ) -> Result<Vec<StateEntry>>`

# Calls

- [email_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/email_state_fingerprint.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [opaque_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [mail_object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries.md)
- [object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)
---
type: Rust Method
title: email_submission_object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L114-L123
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) async fn email_submission_object_state(&self, account_id: Uuid) -> Result<String>`

# Calls

- [email_submission_object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries.md)
- [encode_state_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [handle_email_submission_set](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_email_submission_get](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)
---
type: Rust Method
title: email_submission_object_state_entries
resource: crates/lpe-jmap/src/service/object_state.rs#L125-L140
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/email_submission_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
---

# Signature

`pub(crate) async fn email_submission_object_state_entries( &self, account_id: Uuid, ) -> Result<Vec<StateEntry>>`

# Calls

- [email_submission_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/email_submission_state_fingerprint.md)

# Called by

- [handle_email_submission_changes](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes.md)
- [email_submission_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
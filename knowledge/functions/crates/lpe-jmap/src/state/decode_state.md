---
type: Rust Function
title: decode_state
resource: crates/lpe-jmap/src/state.rs#L355-L364
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/state/state_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
  - functions/crates/lpe-jmap/src/tests/email_state_tokens_do_not_expose_message_or_bcc_content
  - functions/crates/lpe-jmap/src/tests/identity_get_state_tracks_sender_identity_projection
  - functions/crates/lpe-jmap/src/tests/email_submission_get_state_tracks_submission_rows
  - functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor
---

# Signature

`pub(crate) fn decode_state(value: &str) -> Result<StateToken>`

# Called by

- [state_cursor](../../../../../functions/crates/lpe-jmap/src/state/state_cursor.md)
- [changes_response_from_durable_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)
- [email_state_tokens_do_not_expose_message_or_bcc_content](../../../../../functions/crates/lpe-jmap/src/tests/email_state_tokens_do_not_expose_message_or_bcc_content.md)
- [identity_get_state_tracks_sender_identity_projection](../../../../../functions/crates/lpe-jmap/src/tests/identity_get_state_tracks_sender_identity_projection.md)
- [email_submission_get_state_tracks_submission_rows](../../../../../functions/crates/lpe-jmap/src/tests/email_submission_get_state_tracks_submission_rows.md)
- [email_submission_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor.md)
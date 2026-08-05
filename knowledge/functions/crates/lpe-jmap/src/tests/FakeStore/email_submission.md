---
type: Rust Method
title: email_submission
resource: crates/lpe-jmap/src/tests.rs#L807-L820
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  called_by:
  - functions/crates/lpe-jmap/src/tests/email_submission_get_hides_shared_account_without_submit_rights
  - functions/crates/lpe-jmap/src/tests/email_submission_get_state_tracks_submission_rows
  - functions/crates/lpe-jmap/src/tests/email_submission_changes_tracks_submission_rows
  - functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/email_submission_query_filters_sorts_and_reports_query_changes
---

# Signature

`fn email_submission() -> JmapEmailSubmission`

# Calls

- [draft_email](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)

# Called by

- [email_submission_get_hides_shared_account_without_submit_rights](../../../../../../functions/crates/lpe-jmap/src/tests/email_submission_get_hides_shared_account_without_submit_rights.md)
- [email_submission_get_state_tracks_submission_rows](../../../../../../functions/crates/lpe-jmap/src/tests/email_submission_get_state_tracks_submission_rows.md)
- [email_submission_changes_tracks_submission_rows](../../../../../../functions/crates/lpe-jmap/src/tests/email_submission_changes_tracks_submission_rows.md)
- [email_submission_changes_use_durable_log_ids_when_state_has_cursor](../../../../../../functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor.md)
- [email_submission_query_filters_sorts_and_reports_query_changes](../../../../../../functions/crates/lpe-jmap/src/tests/email_submission_query_filters_sorts_and_reports_query_changes.md)
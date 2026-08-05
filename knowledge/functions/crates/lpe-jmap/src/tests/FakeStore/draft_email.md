---
type: Rust Method
title: draft_email
resource: crates/lpe-jmap/src/tests.rs#L410-L463
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/email_submission
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/submit_draft_message
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/copy_jmap_email
  - functions/crates/lpe-jmap/src/tests/jmap_mail_query_snippet_and_blob_projections_do_not_expose_bcc
  - functions/crates/lpe-jmap/src/tests/delegated_email_and_thread_states_ignore_bcc_only_changes
  - functions/crates/lpe-jmap/src/tests/email_changes_report_updates_for_existing_messages
  - functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited
  - functions/crates/lpe-jmap/src/tests/thread_query_changes_reports_added_threads_from_full_snapshot
  - functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations
  - functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state
  - functions/crates/lpe-jmap/src/tests/unsupported_canonical_set_reports_operation_specific_errors
  - functions/crates/lpe-jmap/src/tests/negative_interop_fixtures_reject_invalid_payloads_and_forbidden_writes
---

# Signature

`fn draft_email() -> JmapEmail`

# Called by

- [email_submission](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/email_submission.md)
- [submit_draft_message](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/submit_draft_message.md)
- [copy_jmap_email](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/copy_jmap_email.md)
- [jmap_mail_query_snippet_and_blob_projections_do_not_expose_bcc](../../../../../../functions/crates/lpe-jmap/src/tests/jmap_mail_query_snippet_and_blob_projections_do_not_expose_bcc.md)
- [delegated_email_and_thread_states_ignore_bcc_only_changes](../../../../../../functions/crates/lpe-jmap/src/tests/delegated_email_and_thread_states_ignore_bcc_only_changes.md)
- [email_changes_report_updates_for_existing_messages](../../../../../../functions/crates/lpe-jmap/src/tests/email_changes_report_updates_for_existing_messages.md)
- [thread_query_state_keeps_full_snapshot_when_page_is_limited](../../../../../../functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited.md)
- [thread_query_changes_reports_added_threads_from_full_snapshot](../../../../../../functions/crates/lpe-jmap/src/tests/thread_query_changes_reports_added_threads_from_full_snapshot.md)
- [mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations.md)
- [owned_message_download_prefers_sanitized_stored_raw_mime_blob](../../../../../../functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob.md)
- [scoped_push_change_reports_email_delivery_for_new_messages_only_state](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state.md)
- [unsupported_canonical_set_reports_operation_specific_errors](../../../../../../functions/crates/lpe-jmap/src/tests/unsupported_canonical_set_reports_operation_specific_errors.md)
- [negative_interop_fixtures_reject_invalid_payloads_and_forbidden_writes](../../../../../../functions/crates/lpe-jmap/src/tests/negative_interop_fixtures_reject_invalid_payloads_and_forbidden_writes.md)
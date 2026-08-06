---
type: Rust Function
title: validator_ok
resource: crates/lpe-jmap/src/tests.rs#L147-L161
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/validator_sequence
  called_by:
  - functions/crates/lpe-jmap/src/tests/mailbox_and_email_changes_return_existing_ids_from_initial_state
  - functions/crates/lpe-jmap/src/tests/mailbox_and_email_query_changes_replay_snapshot_differences
  - functions/crates/lpe-jmap/src/tests/paged_query_states_keep_full_mailbox_and_email_snapshots
  - functions/crates/lpe-jmap/src/tests/stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes
  - functions/crates/lpe-jmap/src/tests/stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes
  - functions/crates/lpe-jmap/src/tests/canonical_query_state_tokens_are_scoped_to_each_jmap_surface
  - functions/crates/lpe-jmap/src/tests/stored_mailbox_query_changes_reloads_filtered_snapshot
  - functions/crates/lpe-jmap/src/tests/email_query_changes_exact_limit_does_not_report_more_changes
  - functions/crates/lpe-jmap/src/tests/query_methods_support_anchor_windows_and_missing_anchor_errors
  - functions/crates/lpe-jmap/src/tests/email_query_changes_reports_existing_message_reorders
  - functions/crates/lpe-jmap/src/tests/identity_thread_and_submission_reads_are_available
  - functions/crates/lpe-jmap/src/tests/thread_query_returns_distinct_threads_for_filtered_emails
  - functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited
  - functions/crates/lpe-jmap/src/tests/search_snippets_return_preview_for_requested_messages
  - functions/crates/lpe-jmap/src/tests/mailbox_set_copy_import_and_quota_are_available
  - functions/crates/lpe-jmap/src/tests/email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix
  - functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob
  - functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts
  - functions/crates/lpe-jmap/src/tests/calendar_event_links_write_and_project_canonical_attachments
  - functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account
  - functions/crates/lpe-jmap/src/tests/upload_rejects_bodies_larger_than_session_limit
  - functions/crates/lpe-jmap/src/tests/upload_accepts_validated_matching_blob
  - functions/crates/lpe-jmap/src/tests/upload_rejects_declared_mime_mismatch
---

# Signature

`fn validator_ok( mime_type: &str, label: &str, extension: &str, score: f32, ) -> Validator<FakeDetector>`

# Calls

- [validator_sequence](../../../../../functions/crates/lpe-jmap/src/tests/validator_sequence.md)

# Called by

- [mailbox_and_email_changes_return_existing_ids_from_initial_state](../../../../../functions/crates/lpe-jmap/src/tests/mailbox_and_email_changes_return_existing_ids_from_initial_state.md)
- [mailbox_and_email_query_changes_replay_snapshot_differences](../../../../../functions/crates/lpe-jmap/src/tests/mailbox_and_email_query_changes_replay_snapshot_differences.md)
- [paged_query_states_keep_full_mailbox_and_email_snapshots](../../../../../functions/crates/lpe-jmap/src/tests/paged_query_states_keep_full_mailbox_and_email_snapshots.md)
- [stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes](../../../../../functions/crates/lpe-jmap/src/tests/stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes.md)
- [stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes](../../../../../functions/crates/lpe-jmap/src/tests/stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes.md)
- [canonical_query_state_tokens_are_scoped_to_each_jmap_surface](../../../../../functions/crates/lpe-jmap/src/tests/canonical_query_state_tokens_are_scoped_to_each_jmap_surface.md)
- [stored_mailbox_query_changes_reloads_filtered_snapshot](../../../../../functions/crates/lpe-jmap/src/tests/stored_mailbox_query_changes_reloads_filtered_snapshot.md)
- [email_query_changes_exact_limit_does_not_report_more_changes](../../../../../functions/crates/lpe-jmap/src/tests/email_query_changes_exact_limit_does_not_report_more_changes.md)
- [query_methods_support_anchor_windows_and_missing_anchor_errors](../../../../../functions/crates/lpe-jmap/src/tests/query_methods_support_anchor_windows_and_missing_anchor_errors.md)
- [email_query_changes_reports_existing_message_reorders](../../../../../functions/crates/lpe-jmap/src/tests/email_query_changes_reports_existing_message_reorders.md)
- [identity_thread_and_submission_reads_are_available](../../../../../functions/crates/lpe-jmap/src/tests/identity_thread_and_submission_reads_are_available.md)
- [thread_query_returns_distinct_threads_for_filtered_emails](../../../../../functions/crates/lpe-jmap/src/tests/thread_query_returns_distinct_threads_for_filtered_emails.md)
- [thread_query_state_keeps_full_snapshot_when_page_is_limited](../../../../../functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited.md)
- [search_snippets_return_preview_for_requested_messages](../../../../../functions/crates/lpe-jmap/src/tests/search_snippets_return_preview_for_requested_messages.md)
- [mailbox_set_copy_import_and_quota_are_available](../../../../../functions/crates/lpe-jmap/src/tests/mailbox_set_copy_import_and_quota_are_available.md)
- [email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix](../../../../../functions/crates/lpe-jmap/src/tests/email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix.md)
- [owned_message_download_prefers_sanitized_stored_raw_mime_blob](../../../../../functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob.md)
- [blob_create_paths_reject_read_only_shared_accounts](../../../../../functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts.md)
- [calendar_event_links_write_and_project_canonical_attachments](../../../../../functions/crates/lpe-jmap/src/tests/calendar_event_links_write_and_project_canonical_attachments.md)
- [upload_and_download_use_authenticated_account](../../../../../functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account.md)
- [upload_rejects_bodies_larger_than_session_limit](../../../../../functions/crates/lpe-jmap/src/tests/upload_rejects_bodies_larger_than_session_limit.md)
- [upload_accepts_validated_matching_blob](../../../../../functions/crates/lpe-jmap/src/tests/upload_accepts_validated_matching_blob.md)
- [upload_rejects_declared_mime_mismatch](../../../../../functions/crates/lpe-jmap/src/tests/upload_rejects_declared_mime_mismatch.md)
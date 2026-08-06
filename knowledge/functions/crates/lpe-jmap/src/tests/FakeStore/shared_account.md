---
type: Rust Method
title: shared_account
resource: crates/lpe-jmap/src/tests.rs#L748-L756
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_access
  - functions/crates/lpe-jmap/src/tests/session_and_identity_include_accessible_shared_mailbox_accounts
  - functions/crates/lpe-jmap/src/tests/session_omits_submission_for_shared_mailbox_without_sender_grant
  - functions/crates/lpe-jmap/src/tests/session_omits_submission_for_read_only_shared_mailbox_with_sender_grant
  - functions/crates/lpe-jmap/src/tests/mailbox_changes_report_delegated_submit_right_changes
  - functions/crates/lpe-jmap/src/tests/email_get_hides_bcc_for_delegated_shared_mailbox_projection
  - functions/crates/lpe-jmap/src/tests/changes_reject_malformed_and_cross_account_state_tokens
  - functions/crates/lpe-jmap/src/tests/mailbox_query_states_are_bound_to_the_requested_account
  - functions/crates/lpe-jmap/src/tests/mailbox_query_changes_reject_cross_account_query_state_replay
  - functions/crates/lpe-jmap/src/tests/identity_shared_account_reads_use_the_same_authorization_model
  - functions/crates/lpe-jmap/src/tests/identity_shared_account_reads_reject_inaccessible_accounts
  - functions/crates/lpe-jmap/src/tests/mailbox_get_hides_child_creation_for_read_only_shared_mailboxes
  - functions/crates/lpe-jmap/src/tests/mailbox_get_rename_and_delete_rights_match_mailbox_set
  - functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations
  - functions/crates/lpe-jmap/src/tests/email_copy_allows_shared_source_to_owned_target_without_exposing_bcc
  - functions/crates/lpe-jmap/src/tests/email_copy_rejects_inaccessible_source_and_read_only_target
  - functions/crates/lpe-jmap/src/tests/message_blob_download_hides_bcc_for_delegated_shared_mailbox
  - functions/crates/lpe-jmap/src/tests/blob_copy_to_shared_account_does_not_widen_owner_bcc
  - functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts
  - functions/crates/lpe-jmap/src/tests/websocket_push_states_include_shared_mailbox_accounts
  - functions/crates/lpe-jmap/src/tests/websocket_push_states_include_submission_identity_mail_types
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes
  - functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal
  - functions/crates/lpe-jmap/src/tests/task_list_get_projects_shared_task_list_rights
  - functions/crates/lpe-jmap/src/tests/task_list_changes_tracks_shared_rights_updates
  - functions/crates/lpe-jmap/src/tests/task_query_includes_shared_accessible_tasks
  - functions/crates/lpe-jmap/src/tests/task_set_rejects_writes_to_read_only_shared_task_list
---

# Signature

`fn shared_account() -> AuthenticatedAccount`

# Called by

- [shared_mailbox_access](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_access.md)
- [session_and_identity_include_accessible_shared_mailbox_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/session_and_identity_include_accessible_shared_mailbox_accounts.md)
- [session_omits_submission_for_shared_mailbox_without_sender_grant](../../../../../../functions/crates/lpe-jmap/src/tests/session_omits_submission_for_shared_mailbox_without_sender_grant.md)
- [session_omits_submission_for_read_only_shared_mailbox_with_sender_grant](../../../../../../functions/crates/lpe-jmap/src/tests/session_omits_submission_for_read_only_shared_mailbox_with_sender_grant.md)
- [mailbox_changes_report_delegated_submit_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_changes_report_delegated_submit_right_changes.md)
- [email_get_hides_bcc_for_delegated_shared_mailbox_projection](../../../../../../functions/crates/lpe-jmap/src/tests/email_get_hides_bcc_for_delegated_shared_mailbox_projection.md)
- [changes_reject_malformed_and_cross_account_state_tokens](../../../../../../functions/crates/lpe-jmap/src/tests/changes_reject_malformed_and_cross_account_state_tokens.md)
- [mailbox_query_states_are_bound_to_the_requested_account](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_query_states_are_bound_to_the_requested_account.md)
- [mailbox_query_changes_reject_cross_account_query_state_replay](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_query_changes_reject_cross_account_query_state_replay.md)
- [identity_shared_account_reads_use_the_same_authorization_model](../../../../../../functions/crates/lpe-jmap/src/tests/identity_shared_account_reads_use_the_same_authorization_model.md)
- [identity_shared_account_reads_reject_inaccessible_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/identity_shared_account_reads_reject_inaccessible_accounts.md)
- [mailbox_get_hides_child_creation_for_read_only_shared_mailboxes](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_get_hides_child_creation_for_read_only_shared_mailboxes.md)
- [mailbox_get_rename_and_delete_rights_match_mailbox_set](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_get_rename_and_delete_rights_match_mailbox_set.md)
- [mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations.md)
- [email_copy_allows_shared_source_to_owned_target_without_exposing_bcc](../../../../../../functions/crates/lpe-jmap/src/tests/email_copy_allows_shared_source_to_owned_target_without_exposing_bcc.md)
- [email_copy_rejects_inaccessible_source_and_read_only_target](../../../../../../functions/crates/lpe-jmap/src/tests/email_copy_rejects_inaccessible_source_and_read_only_target.md)
- [message_blob_download_hides_bcc_for_delegated_shared_mailbox](../../../../../../functions/crates/lpe-jmap/src/tests/message_blob_download_hides_bcc_for_delegated_shared_mailbox.md)
- [blob_copy_to_shared_account_does_not_widen_owner_bcc](../../../../../../functions/crates/lpe-jmap/src/tests/blob_copy_to_shared_account_does_not_widen_owner_bcc.md)
- [blob_create_paths_reject_read_only_shared_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts.md)
- [websocket_push_states_include_shared_mailbox_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_states_include_shared_mailbox_accounts.md)
- [websocket_push_states_include_submission_identity_mail_types](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_states_include_submission_identity_mail_types.md)
- [websocket_push_enable_sends_full_state_for_missing_or_stale_push_state](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
- [scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes.md)
- [scoped_push_change_reports_delegated_mailbox_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes.md)
- [scoped_push_change_reports_delegated_identity_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes.md)
- [shared_task_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal.md)
- [shared_task_list_rights_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal.md)
- [deleted_shared_task_list_push_change_wakes_former_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal.md)
- [task_list_get_projects_shared_task_list_rights](../../../../../../functions/crates/lpe-jmap/src/tests/task_list_get_projects_shared_task_list_rights.md)
- [task_list_changes_tracks_shared_rights_updates](../../../../../../functions/crates/lpe-jmap/src/tests/task_list_changes_tracks_shared_rights_updates.md)
- [task_query_includes_shared_accessible_tasks](../../../../../../functions/crates/lpe-jmap/src/tests/task_query_includes_shared_accessible_tasks.md)
- [task_set_rejects_writes_to_read_only_shared_task_list](../../../../../../functions/crates/lpe-jmap/src/tests/task_set_rejects_writes_to_read_only_shared_task_list.md)
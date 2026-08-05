---
type: Rust Function
title: send_command
resource: crates/lpe-imap/src/tests.rs#L4019-L4023
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/read_response
  called_by:
  - functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work
  - functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting
  - functions/crates/lpe-imap/src/tests/unicode_mailbox_commands_resolve_and_render_consistently
  - functions/crates/lpe-imap/src/tests/real_client_like_unicode_mailbox_transcript_covers_display_names
  - functions/crates/lpe-imap/src/tests/subscribe_unsubscribe_and_lsub_use_persisted_mailbox_state
  - functions/crates/lpe-imap/src/tests/unicode_nested_paths_and_list_wildcards_work_by_segment
  - functions/crates/lpe-imap/src/tests/imap_nested_rename_moves_parent_and_final_segment
  - functions/crates/lpe-imap/src/tests/malformed_utf8_mailbox_paths_are_rejected
  - functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation
  - functions/crates/lpe-imap/src/tests/malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly
  - functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented
  - functions/crates/lpe-imap/src/tests/unicode_spoofing_duplicates_are_rejected_for_imap_create_and_rename
  - functions/crates/lpe-imap/src/tests/mailbox_aliases_discover_and_select_canonical_special_folders
  - functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript
  - functions/crates/lpe-imap/src/tests/thunderbird_copy_to_trash_then_expunge_removes_source_only
  - functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy
  - functions/crates/lpe-imap/src/tests/store_and_uid_store_update_only_canonical_supported_flags
  - functions/crates/lpe-imap/src/tests/append_copy_move_and_expunge_preserve_canonical_uid_state
  - functions/crates/lpe-imap/src/tests/unselect_keeps_deleted_messages_until_explicit_expunge
  - functions/crates/lpe-imap/src/tests/close_expunges_deleted_in_read_write_mailbox_without_untagged_expunge
  - functions/crates/lpe-imap/src/tests/outlook_uid_search_refreshes_selected_mailbox_before_fetch
  - functions/crates/lpe-imap/src/tests/outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable
  - functions/crates/lpe-imap/src/tests/condstore_store_reports_modified_and_keeps_fresh_messages
  - functions/crates/lpe-imap/src/tests/condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised
  - functions/crates/lpe-imap/src/tests/search_and_uid_search_use_canonical_visible_fields_without_bcc
  - functions/crates/lpe-imap/src/tests/inbox_fetch_and_search_do_not_leak_bcc
  - functions/crates/lpe-imap/src/tests/sent_fetch_does_not_expose_protected_bcc
  - functions/crates/lpe-imap/src/tests/fetch_renders_canonical_multipart_mime_without_bcc
  - functions/crates/lpe-imap/src/tests/noop_and_check_emit_selected_mailbox_refresh_updates
  - functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state
  - functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes
  - functions/crates/lpe-imap/src/tests/store_survives_concurrent_selected_mailbox_removal
  - functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change
  - functions/crates/lpe-imap/src/tests/idle_without_selected_mailbox_is_noop_for_outlook
  - functions/crates/lpe-imap/src/tests/xoauth2_authenticate_is_accepted
  - functions/crates/lpe-imap/src/tests/plain_authenticate_with_initial_response_is_accepted
  - functions/crates/lpe-imap/src/tests/plain_authenticate_challenge_response_is_accepted
  - functions/crates/lpe-imap/src/tests/login_authenticate_challenge_response_is_accepted
  - functions/crates/lpe-imap/src/tests/login_authenticate_with_initial_username_is_accepted
  - functions/crates/lpe-imap/src/tests/legacy_auth_login_alias_is_accepted
  - functions/crates/lpe-imap/src/tests/login_accepts_username_and_password_literals
  - functions/crates/lpe-imap/src/tests/authenticate_login_accepts_initial_username_literal
  - functions/crates/lpe-imap/src/tests/quota_probe_commands_are_tolerated_for_outlook_setup
  - functions/crates/lpe-imap/src/tests/acl_commands_project_canonical_mailbox_and_sender_delegation
---

# Signature

`async fn send_command(stream: &mut TcpStream, command: &str, tag: &str) -> String`

# Calls

- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)

# Called by

- [login_list_select_fetch_store_search_and_append_work](../../../../../functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work.md)
- [utf8_accept_enables_utf8_mailbox_response_quoting](../../../../../functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting.md)
- [unicode_mailbox_commands_resolve_and_render_consistently](../../../../../functions/crates/lpe-imap/src/tests/unicode_mailbox_commands_resolve_and_render_consistently.md)
- [real_client_like_unicode_mailbox_transcript_covers_display_names](../../../../../functions/crates/lpe-imap/src/tests/real_client_like_unicode_mailbox_transcript_covers_display_names.md)
- [subscribe_unsubscribe_and_lsub_use_persisted_mailbox_state](../../../../../functions/crates/lpe-imap/src/tests/subscribe_unsubscribe_and_lsub_use_persisted_mailbox_state.md)
- [unicode_nested_paths_and_list_wildcards_work_by_segment](../../../../../functions/crates/lpe-imap/src/tests/unicode_nested_paths_and_list_wildcards_work_by_segment.md)
- [imap_nested_rename_moves_parent_and_final_segment](../../../../../functions/crates/lpe-imap/src/tests/imap_nested_rename_moves_parent_and_final_segment.md)
- [malformed_utf8_mailbox_paths_are_rejected](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_mailbox_paths_are_rejected.md)
- [malformed_utf8_command_literals_are_rejected_before_mailbox_validation](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation.md)
- [malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly.md)
- [append_message_literals_remain_byte_oriented](../../../../../functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented.md)
- [unicode_spoofing_duplicates_are_rejected_for_imap_create_and_rename](../../../../../functions/crates/lpe-imap/src/tests/unicode_spoofing_duplicates_are_rejected_for_imap_create_and_rename.md)
- [mailbox_aliases_discover_and_select_canonical_special_folders](../../../../../functions/crates/lpe-imap/src/tests/mailbox_aliases_discover_and_select_canonical_special_folders.md)
- [outlook_first_login_list_select_sync_transcript](../../../../../functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript.md)
- [thunderbird_copy_to_trash_then_expunge_removes_source_only](../../../../../functions/crates/lpe-imap/src/tests/thunderbird_copy_to_trash_then_expunge_removes_source_only.md)
- [thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy](../../../../../functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy.md)
- [store_and_uid_store_update_only_canonical_supported_flags](../../../../../functions/crates/lpe-imap/src/tests/store_and_uid_store_update_only_canonical_supported_flags.md)
- [append_copy_move_and_expunge_preserve_canonical_uid_state](../../../../../functions/crates/lpe-imap/src/tests/append_copy_move_and_expunge_preserve_canonical_uid_state.md)
- [unselect_keeps_deleted_messages_until_explicit_expunge](../../../../../functions/crates/lpe-imap/src/tests/unselect_keeps_deleted_messages_until_explicit_expunge.md)
- [close_expunges_deleted_in_read_write_mailbox_without_untagged_expunge](../../../../../functions/crates/lpe-imap/src/tests/close_expunges_deleted_in_read_write_mailbox_without_untagged_expunge.md)
- [outlook_uid_search_refreshes_selected_mailbox_before_fetch](../../../../../functions/crates/lpe-imap/src/tests/outlook_uid_search_refreshes_selected_mailbox_before_fetch.md)
- [outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable](../../../../../functions/crates/lpe-imap/src/tests/outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable.md)
- [condstore_store_reports_modified_and_keeps_fresh_messages](../../../../../functions/crates/lpe-imap/src/tests/condstore_store_reports_modified_and_keeps_fresh_messages.md)
- [condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised](../../../../../functions/crates/lpe-imap/src/tests/condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised.md)
- [search_and_uid_search_use_canonical_visible_fields_without_bcc](../../../../../functions/crates/lpe-imap/src/tests/search_and_uid_search_use_canonical_visible_fields_without_bcc.md)
- [inbox_fetch_and_search_do_not_leak_bcc](../../../../../functions/crates/lpe-imap/src/tests/inbox_fetch_and_search_do_not_leak_bcc.md)
- [sent_fetch_does_not_expose_protected_bcc](../../../../../functions/crates/lpe-imap/src/tests/sent_fetch_does_not_expose_protected_bcc.md)
- [fetch_renders_canonical_multipart_mime_without_bcc](../../../../../functions/crates/lpe-imap/src/tests/fetch_renders_canonical_multipart_mime_without_bcc.md)
- [noop_and_check_emit_selected_mailbox_refresh_updates](../../../../../functions/crates/lpe-imap/src/tests/noop_and_check_emit_selected_mailbox_refresh_updates.md)
- [reconnect_select_refreshes_from_canonical_mailbox_state](../../../../../functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state.md)
- [idle_reports_selected_mailbox_flag_changes](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes.md)
- [store_survives_concurrent_selected_mailbox_removal](../../../../../functions/crates/lpe-imap/src/tests/store_survives_concurrent_selected_mailbox_removal.md)
- [idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change.md)
- [idle_without_selected_mailbox_is_noop_for_outlook](../../../../../functions/crates/lpe-imap/src/tests/idle_without_selected_mailbox_is_noop_for_outlook.md)
- [xoauth2_authenticate_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/xoauth2_authenticate_is_accepted.md)
- [plain_authenticate_with_initial_response_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/plain_authenticate_with_initial_response_is_accepted.md)
- [plain_authenticate_challenge_response_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/plain_authenticate_challenge_response_is_accepted.md)
- [login_authenticate_challenge_response_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/login_authenticate_challenge_response_is_accepted.md)
- [login_authenticate_with_initial_username_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/login_authenticate_with_initial_username_is_accepted.md)
- [legacy_auth_login_alias_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/legacy_auth_login_alias_is_accepted.md)
- [login_accepts_username_and_password_literals](../../../../../functions/crates/lpe-imap/src/tests/login_accepts_username_and_password_literals.md)
- [authenticate_login_accepts_initial_username_literal](../../../../../functions/crates/lpe-imap/src/tests/authenticate_login_accepts_initial_username_literal.md)
- [quota_probe_commands_are_tolerated_for_outlook_setup](../../../../../functions/crates/lpe-imap/src/tests/quota_probe_commands_are_tolerated_for_outlook_setup.md)
- [acl_commands_project_canonical_mailbox_and_sender_delegation](../../../../../functions/crates/lpe-imap/src/tests/acl_commands_project_canonical_mailbox_and_sender_delegation.md)
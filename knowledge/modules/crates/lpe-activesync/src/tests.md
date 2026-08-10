---
type: Rust Module
title: tests
resource: crates/lpe-activesync/src/tests.rs#L1-L6258
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap-hashset-sync-arc-mutex-time-instant
  - external/anyhow-anyhow
  - external/argon2-password-hash-rand-core-osrng-saltstring-argon2-passwordhasher
  - external/axum-body-to-bytes
  - external/axum-http-headermap-headervalue-statuscode
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-accountlogin-activesyncattachment-activesyncattachmentcontent-activesyncdevicestate-activesyncitemstate-activesyncsyncstate-auditentryinput-authenticatedaccount-clientcontact-clientevent-jmapemail-jmapemailaddress-jmapemailfollowupupdate-jmapemailmailboxstate-jmapemailquery-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-jmapuploadblob-mailboxaccountaccess-saveddraftmessage-storedaccountapppassword-submitmessageinput-submittedmessage-upsertclientcontactinput-upsertclienteventinput
  - external/uuid-uuid
  - external/crate-app-options-response-for-store-response-error-response-service-activesyncservice-store-activesyncstore-storefuture-types-activesyncquery-parsedactivesyncquery-wbxml-decode-wbxml-encode-wbxml-wbxmlnode
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [FakeStore](../../../../classes/crates/lpe-activesync/src/tests/FakeStore.md)
- [tenant_id](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/tenant_id.md)
- [account](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/account.md)
- [password_hash](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/password_hash.md)
- [login](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/login.md)
- [draft_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/draft_mailbox.md)
- [inbox_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/inbox_mailbox.md)
- [sent_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/sent_mailbox.md)
- [mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/mailbox.md)
- [inbox_email](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/inbox_email.md)
- [mailbox_access](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/mailbox_access.md)
- [shared_mailbox_access](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/shared_mailbox_access.md)
- [device_key](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)
- [current_mailboxes](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes.md)
- [set_current_mailboxes](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/set_current_mailboxes.md)
- [fetch_account_session](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/accountauthstore/fetch_account_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/accountauthstore/fetch_account_login.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/accountauthstore/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/accountauthstore/touch_account_app_password.md)
- [append_audit_event](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/accountauthstore/append_audit_event.md)
- [fetch_accessible_mailbox_accounts](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_accessible_mailbox_accounts.md)
- [fetch_jmap_mailboxes](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_mailboxes.md)
- [create_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/destroy_jmap_mailbox.md)
- [query_jmap_email_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/query_jmap_email_ids.md)
- [fetch_jmap_emails](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_emails.md)
- [fetch_latest_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_latest_activesync_sync_state.md)
- [fetch_activesync_device](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_device.md)
- [store_activesync_device_pending_policy](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/store_activesync_device_pending_policy.md)
- [acknowledge_activesync_device_policy](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/acknowledge_activesync_device_policy.md)
- [touch_activesync_device](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/touch_activesync_device.md)
- [create_canonical_change_listener](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_canonical_change_listener.md)
- [fetch_jmap_draft](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_draft.md)
- [fetch_jmap_message_blob](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_message_blob.md)
- [move_jmap_email_from_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/move_jmap_email_from_mailbox.md)
- [delete_jmap_email_from_mailbox](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/delete_jmap_email_from_mailbox.md)
- [update_jmap_email_flags](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_email_flags.md)
- [update_jmap_email_followup_flags](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_email_followup_flags.md)
- [fetch_activesync_message_attachments](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_message_attachments.md)
- [fetch_activesync_attachment_content](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_attachment_content.md)
- [fetch_activesync_email_states](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_email_states.md)
- [fetch_activesync_email_states_by_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_email_states_by_ids.md)
- [save_draft_message](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/save_draft_message.md)
- [delete_draft_message](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/delete_draft_message.md)
- [submit_message](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/submit_message.md)
- [fetch_client_contacts](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_client_contacts.md)
- [fetch_client_contacts_by_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_client_contacts_by_ids.md)
- [upsert_client_contact](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/upsert_client_contact.md)
- [delete_client_contact](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/delete_client_contact.md)
- [fetch_client_events](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_client_events.md)
- [fetch_client_events_by_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_client_events_by_ids.md)
- [upsert_client_event](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/upsert_client_event.md)
- [delete_client_event](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/delete_client_event.md)
- [fetch_activesync_contact_states](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_contact_states.md)
- [fetch_activesync_contact_states_by_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_contact_states_by_ids.md)
- [fetch_activesync_event_states](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_event_states.md)
- [fetch_activesync_event_states_by_ids](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_event_states_by_ids.md)
- [store_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/store_activesync_sync_state.md)
- [fetch_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_activesync_sync_state.md)
- [cleanup_expired_activesync_sync_cursors](../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/cleanup_expired_activesync_sync_cursors.md)
- [bearer_headers](../../../../functions/crates/lpe-activesync/src/tests/bearer_headers.md)
- [basic_headers](../../../../functions/crates/lpe-activesync/src/tests/basic_headers.md)
- [bearer_headers_with_protocol_version](../../../../functions/crates/lpe-activesync/src/tests/bearer_headers_with_protocol_version.md)
- [mime_headers](../../../../functions/crates/lpe-activesync/src/tests/mime_headers.md)
- [options_challenges_anonymous_requests](../../../../functions/crates/lpe-activesync/src/tests/options_challenges_anonymous_requests.md)
- [options_returns_capabilities_after_authentication](../../../../functions/crates/lpe-activesync/src/tests/options_returns_capabilities_after_authentication.md)
- [post_authentication_errors_return_http_challenge](../../../../functions/crates/lpe-activesync/src/tests/post_authentication_errors_return_http_challenge.md)
- [post_with_supported_protocol_version_succeeds](../../../../functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds.md)
- [post_with_unsupported_protocol_version_is_rejected_predictably](../../../../functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably.md)
- [unsupported_protocol_version_response_does_not_echo_request_version](../../../../functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version.md)
- [provision_returns_policy_key_and_lightweight_policy_document](../../../../functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)
- [enforced_mode_validates_later_command_policy_key](../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)
- [permissive_mode_preserves_current_unprovisioned_behavior](../../../../functions/crates/lpe-activesync/src/tests/permissive_mode_preserves_current_unprovisioned_behavior.md)
- [provision_request](../../../../functions/crates/lpe-activesync/src/tests/provision_request.md)
- [folder_sync_request](../../../../functions/crates/lpe-activesync/src/tests/folder_sync_request.md)
- [folder_create_request](../../../../functions/crates/lpe-activesync/src/tests/folder_create_request.md)
- [folder_update_request](../../../../functions/crates/lpe-activesync/src/tests/folder_update_request.md)
- [folder_delete_request](../../../../functions/crates/lpe-activesync/src/tests/folder_delete_request.md)
- [folder_command](../../../../functions/crates/lpe-activesync/src/tests/folder_command.md)
- [decode_response_body](../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)
- [collection_sync_key](../../../../functions/crates/lpe-activesync/src/tests/collection_sync_key.md)
- [active_sync_query](../../../../functions/crates/lpe-activesync/src/tests/active_sync_query.md)
- [base64_query](../../../../functions/crates/lpe-activesync/src/tests/base64_query.md)
- [base64_query_with_version](../../../../functions/crates/lpe-activesync/src/tests/base64_query_with_version.md)
- [parsed_base64_query](../../../../functions/crates/lpe-activesync/src/tests/parsed_base64_query.md)
- [handle_base64_request](../../../../functions/crates/lpe-activesync/src/tests/handle_base64_request.md)
- [base64_query_decodes_ashttp_fields](../../../../functions/crates/lpe-activesync/src/tests/base64_query_decodes_ashttp_fields.md)
- [plain_query_parsing_keeps_existing_fields](../../../../functions/crates/lpe-activesync/src/tests/plain_query_parsing_keeps_existing_fields.md)
- [malformed_base64_query_is_rejected_predictably](../../../../functions/crates/lpe-activesync/src/tests/malformed_base64_query_is_rejected_predictably.md)
- [base64_query_rejects_unsupported_protocol_version](../../../../functions/crates/lpe-activesync/src/tests/base64_query_rejects_unsupported_protocol_version.md)
- [sync_collection](../../../../functions/crates/lpe-activesync/src/tests/sync_collection.md)
- [only_sync_collection](../../../../functions/crates/lpe-activesync/src/tests/only_sync_collection.md)
- [first_sync_collection](../../../../functions/crates/lpe-activesync/src/tests/first_sync_collection.md)
- [status_value](../../../../functions/crates/lpe-activesync/src/tests/status_value.md)
- [folder_sync](../../../../functions/crates/lpe-activesync/src/tests/folder_sync.md)
- [ping_request](../../../../functions/crates/lpe-activesync/src/tests/ping_request.md)
- [ping](../../../../functions/crates/lpe-activesync/src/tests/ping.md)
- [folder_add](../../../../functions/crates/lpe-activesync/src/tests/folder_add.md)
- [handle_sync_node](../../../../functions/crates/lpe-activesync/src/tests/handle_sync_node.md)
- [base64_sync_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches.md)
- [base64_ping_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches.md)
- [base64_send_mail_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches.md)
- [base64_smart_reply_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches.md)
- [base64_smart_forward_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches.md)
- [base64_move_items_request_dispatches](../../../../functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches.md)
- [sync_missing_and_invalid_collection_ids_return_status_nodes](../../../../functions/crates/lpe-activesync/src/tests/sync_missing_and_invalid_collection_ids_return_status_nodes.md)
- [folder_sync_stale_key_returns_status_9](../../../../functions/crates/lpe-activesync/src/tests/folder_sync_stale_key_returns_status_9.md)
- [move_items_invalid_source_and_destination_return_item_statuses](../../../../functions/crates/lpe-activesync/src/tests/move_items_invalid_source_and_destination_return_item_statuses.md)
- [item_operations_missing_and_unknown_file_reference_return_attachment_status](../../../../functions/crates/lpe-activesync/src/tests/item_operations_missing_and_unknown_file_reference_return_attachment_status.md)
- [search_malformed_range_returns_store_status_2](../../../../functions/crates/lpe-activesync/src/tests/search_malformed_range_returns_store_status_2.md)
- [ping_recoverable_errors_return_documented_statuses](../../../../functions/crates/lpe-activesync/src/tests/ping_recoverable_errors_return_documented_statuses.md)
- [unsupported_sync_child_command_returns_protocol_status](../../../../functions/crates/lpe-activesync/src/tests/unsupported_sync_child_command_returns_protocol_status.md)
- [one_collection_sync](../../../../functions/crates/lpe-activesync/src/tests/one_collection_sync.md)
- [wbxml_roundtrip_preserves_tokens_and_text](../../../../functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_tokens_and_text.md)
- [wbxml_decode_preserves_unsupported_enum_boundaries](../../../../functions/crates/lpe-activesync/src/tests/wbxml_decode_preserves_unsupported_enum_boundaries.md)
- [wbxml_roundtrip_preserves_get_item_estimate_tokens](../../../../functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_get_item_estimate_tokens.md)
- [move_items_moves_message_between_canonical_mail_folders](../../../../functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders.md)
- [sync_delete_moves_message_to_trash_by_default](../../../../functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default.md)
- [sync_change_updates_read_state_and_round_trips](../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips.md)
- [sync_change_updates_followup_flag_state](../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [folder_sync_returns_mail_and_collaboration_collections](../../../../functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections.md)
- [folder_sync_preserves_nested_mailbox_parent_ids](../../../../functions/crates/lpe-activesync/src/tests/folder_sync_preserves_nested_mailbox_parent_ids.md)
- [folder_sync_projects_shared_mailbox_folders_with_hierarchy](../../../../functions/crates/lpe-activesync/src/tests/folder_sync_projects_shared_mailbox_folders_with_hierarchy.md)
- [stale_folder_sync_key_is_rejected_after_completed_round](../../../../functions/crates/lpe-activesync/src/tests/stale_folder_sync_key_is_rejected_after_completed_round.md)
- [folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key](../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_update_renames_custom_mail_folder](../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
- [folder_delete_deletes_custom_mail_folder](../../../../functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
- [folder_mutation_with_stale_hierarchy_key_is_rejected](../../../../functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
- [sync_add_command_saves_draft_through_canonical_storage](../../../../functions/crates/lpe-activesync/src/tests/sync_add_command_saves_draft_through_canonical_storage.md)
- [sync_handles_multiple_collections_and_common_optional_tokens](../../../../functions/crates/lpe-activesync/src/tests/sync_handles_multiple_collections_and_common_optional_tokens.md)
- [sync_key_zero_primes_then_returns_paged_more_available_changes](../../../../functions/crates/lpe-activesync/src/tests/sync_key_zero_primes_then_returns_paged_more_available_changes.md)
- [get_item_estimate_returns_pending_sync_count](../../../../functions/crates/lpe-activesync/src/tests/get_item_estimate_returns_pending_sync_count.md)
- [stable_sync_does_not_reload_full_email_payloads_without_changes](../../../../functions/crates/lpe-activesync/src/tests/stable_sync_does_not_reload_full_email_payloads_without_changes.md)
- [sync_key_stays_usable_for_new_changes_after_a_stable_round](../../../../functions/crates/lpe-activesync/src/tests/sync_key_stays_usable_for_new_changes_after_a_stable_round.md)
- [stale_sync_key_is_rejected_after_a_completed_round](../../../../functions/crates/lpe-activesync/src/tests/stale_sync_key_is_rejected_after_a_completed_round.md)
- [restart_safe_no_change_sync_keeps_persisted_key_usable](../../../../functions/crates/lpe-activesync/src/tests/restart_safe_no_change_sync_keeps_persisted_key_usable.md)
- [unknown_sync_key_is_rejected_with_invalid_sync_key_status](../../../../functions/crates/lpe-activesync/src/tests/unknown_sync_key_is_rejected_with_invalid_sync_key_status.md)
- [expired_sync_key_is_cleaned_up_and_rejected](../../../../functions/crates/lpe-activesync/src/tests/expired_sync_key_is_cleaned_up_and_rejected.md)
- [superseded_incomplete_sync_key_is_rejected](../../../../functions/crates/lpe-activesync/src/tests/superseded_incomplete_sync_key_is_rejected.md)
- [hierarchy_change_after_existing_sync_returns_folder_sync_required](../../../../functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required.md)
- [send_mail_uses_canonical_submission_model](../../../../functions/crates/lpe-activesync/src/tests/send_mail_uses_canonical_submission_model.md)
- [send_mail_uses_on_behalf_sender_for_delegated_mailbox](../../../../functions/crates/lpe-activesync/src/tests/send_mail_uses_on_behalf_sender_for_delegated_mailbox.md)
- [send_mail_rejects_inaccessible_shared_mailbox_address](../../../../functions/crates/lpe-activesync/src/tests/send_mail_rejects_inaccessible_shared_mailbox_address.md)
- [send_mail_decodes_multipart_and_encoded_headers](../../../../functions/crates/lpe-activesync/src/tests/send_mail_decodes_multipart_and_encoded_headers.md)
- [basic_authentication_is_accepted](../../../../functions/crates/lpe-activesync/src/tests/basic_authentication_is_accepted.md)
- [mime_parser_extracts_attachments_for_sendmail_submission](../../../../functions/crates/lpe-activesync/src/tests/mime_parser_extracts_attachments_for_sendmail_submission.md)
- [item_operations_fetch_returns_attachment_bytes](../../../../functions/crates/lpe-activesync/src/tests/item_operations_fetch_returns_attachment_bytes.md)
- [search_queries_canonical_mail_projection](../../../../functions/crates/lpe-activesync/src/tests/search_queries_canonical_mail_projection.md)
- [sync_projects_email_followup_flag_state](../../../../functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state.md)
- [ping_reports_changed_collections_after_sync_state_exists](../../../../functions/crates/lpe-activesync/src/tests/ping_reports_changed_collections_after_sync_state_exists.md)
- [ping_reconnects_after_service_restart_using_persisted_sync_state](../../../../functions/crates/lpe-activesync/src/tests/ping_reconnects_after_service_restart_using_persisted_sync_state.md)
- [ping_rejects_unsynchronized_folders](../../../../functions/crates/lpe-activesync/src/tests/ping_rejects_unsynchronized_folders.md)
- [ping_empty_request_without_cached_parameters_returns_missing_parameters](../../../../functions/crates/lpe-activesync/src/tests/ping_empty_request_without_cached_parameters_returns_missing_parameters.md)
- [ping_invalid_folder_id_requires_folder_sync](../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_id_requires_folder_sync.md)
- [ping_invalid_folder_class_requires_folder_sync](../../../../functions/crates/lpe-activesync/src/tests/ping_invalid_folder_class_requires_folder_sync.md)
- [ping_no_changes_returns_no_change_status](../../../../functions/crates/lpe-activesync/src/tests/ping_no_changes_returns_no_change_status.md)
- [ping_reports_changed_folder_ids_as_folder_values](../../../../functions/crates/lpe-activesync/src/tests/ping_reports_changed_folder_ids_as_folder_values.md)
- [ping_detects_changes_across_multiple_monitored_collections](../../../../functions/crates/lpe-activesync/src/tests/ping_detects_changes_across_multiple_monitored_collections.md)
- [ping_heartbeat_outside_supported_range_returns_limit](../../../../functions/crates/lpe-activesync/src/tests/ping_heartbeat_outside_supported_range_returns_limit.md)
- [ping_too_many_monitored_folders_returns_max_folders](../../../../functions/crates/lpe-activesync/src/tests/ping_too_many_monitored_folders_returns_max_folders.md)
- [ping_surfaces_hierarchy_change_as_folder_sync_required](../../../../functions/crates/lpe-activesync/src/tests/ping_surfaces_hierarchy_change_as_folder_sync_required.md)
- [smart_reply_uses_source_recipients_and_canonical_submission](../../../../functions/crates/lpe-activesync/src/tests/smart_reply_uses_source_recipients_and_canonical_submission.md)
- [smart_forward_reuses_source_message_and_attachments](../../../../functions/crates/lpe-activesync/src/tests/smart_forward_reuses_source_message_and_attachments.md)
- [sync_contact_and_calendar_mutations_update_canonical_models](../../../../functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_mutations_update_canonical_models.md)
- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)
- [sync_contact_and_calendar_projection_includes_supported_application_data](../../../../functions/crates/lpe-activesync/src/tests/sync_contact_and_calendar_projection_includes_supported_application_data.md)
- [sync_commands_node](../../../../functions/crates/lpe-activesync/src/tests/sync_commands_node.md)
- [sync_add](../../../../functions/crates/lpe-activesync/src/tests/sync_add.md)
- [sync_change](../../../../functions/crates/lpe-activesync/src/tests/sync_change.md)
- [sync_delete](../../../../functions/crates/lpe-activesync/src/tests/sync_delete.md)
- [benchmark_sync_refresh_and_submission_paths](../../../../functions/crates/lpe-activesync/src/tests/benchmark_sync_refresh_and_submission_paths.md)
- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Imports

- `std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
}`
- `anyhow::anyhow`
- `argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
}`
- `axum::body::to_bytes`
- `axum::http::{HeaderMap, HeaderValue, StatusCode}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{
    AccountLogin, ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncDeviceState,
    ActiveSyncItemState, ActiveSyncSyncState, AuditEntryInput, AuthenticatedAccount, ClientContact,
    ClientEvent, JmapEmail, JmapEmailAddress, JmapEmailFollowupUpdate, JmapEmailMailboxState,
    JmapEmailQuery, JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapUploadBlob,
    MailboxAccountAccess, SavedDraftMessage, StoredAccountAppPassword, SubmitMessageInput,
    SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput,
}`
- `uuid::Uuid`
- `crate::{
    app::options_response_for_store,
    response::error_response,
    service::ActiveSyncService,
    store::{ActiveSyncStore, StoreFuture},
    types::{ActiveSyncQuery, ParsedActiveSyncQuery},
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)
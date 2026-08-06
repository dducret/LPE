---
type: Rust Module
title: app
resource: crates/lpe-admin-api/src/app.rs#L1-L1010
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-defaultbodylimit-middleware-routing-delete-get-patch-post-put-router
  - external/lpe-storage-storage
  - external/pub-use-crate-bootstrap-bootstrap-admin-bootstrap-admin-request-from-env-bootstrap-admin-request-from-env-or-defaults-integration-shared-secret
  - external/pub-use-crate-readiness-ha-allows-active-work-ha-current-role
  - external/crate-admin-auth-admin-auth-factors-enroll-totp-login-logout-me-oidc-callback-oidc-metadata-oidc-start-revoke-admin-factor-verify-totp-factor-client-auth-account-auth-factors-client-login-client-logout-client-me-client-oidc-callback-client-oidc-metadata-client-oidc-start-create-account-app-password-create-client-oauth-access-token-enroll-account-totp-list-account-app-passwords-revoke-account-app-password-revoke-account-factor-verify-account-totp-factor-client-config-console-attachment-support-create-account-create-alias-create-domain-create-filter-rule-create-mailbox-create-pst-transfer-job-create-server-administrator-dashboard-local-ai-health-mail-flow-run-pst-jobs-search-email-trace-update-account-update-antispam-settings-update-domain-update-local-ai-settings-update-security-settings-update-server-settings-upload-pst-import-delegation-delete-calendar-collection-grant-delete-collaboration-grant-delete-mailbox-delegation-grant-delete-sender-delegation-grant-delete-task-list-grant-get-free-busy-get-mailbox-delegation-list-collaboration-overview-upsert-calendar-collection-grant-upsert-collaboration-grant-upsert-mailbox-delegation-grant-upsert-sender-delegation-grant-upsert-task-list-grant-health-health-health-live-health-ready-integration-accept-smtp-submission-authenticate-smtp-submission-deliver-inbound-message-verify-lpe-ct-recipient-observability-pst-pst-upload-max-bytes-sieve-delete-sieve-script-get-sieve-overview-get-sieve-script-list-mailbox-rules-put-sieve-script-rename-sieve-script-set-active-sieve-script-snapshots-create-snapshot-delete-snapshot-list-snapshots-restore-snapshot-storage-create-storage-pool-get-storage-cleanup-get-storage-health-get-storage-migrations-get-storage-policies-list-storage-pools-update-account-storage-policy-update-domain-storage-policy-update-platform-storage-policy-update-storage-pool-update-tenant-storage-policy-workspace-client-workspace-create-public-folder-child-create-public-folder-tree-delete-client-contact-delete-client-event-delete-client-note-delete-client-task-delete-draft-message-delete-journal-entry-delete-public-folder-delete-public-folder-item-delete-public-folder-permission-delete-public-folder-replica-delete-search-folder-dismiss-recipient-suggestion-get-client-contact-get-client-note-get-client-task-get-journal-entry-get-public-folder-get-search-folder-list-client-contacts-list-client-notes-list-client-task-lists-list-client-tasks-list-contact-books-list-journal-entries-list-public-folder-children-list-public-folder-items-list-public-folder-per-user-state-list-public-folder-permissions-list-public-folder-replicas-list-public-folder-trees-list-recoverable-items-list-search-folders-outlook-profile-state-patch-client-contact-patch-public-folder-item-patch-public-folder-per-user-state-post-public-folder-item-purge-recoverable-item-put-public-folder-permission-put-public-folder-replica-query-client-reminders-query-recipient-suggestions-restore-recoverable-item-save-draft-message-submit-message-update-message-flag-update-public-folder-upsert-client-contact-upsert-client-event-upsert-client-note-upsert-client-task-upsert-journal-entry-upsert-search-folder
  - external/super-bootstrap-admin-request-from-env-bootstrap-admin-request-from-env-or-defaults-ha-allows-active-work-integration-shared-secret
  - external/crate-ha-activation-check
  - external/crate-integration-build-smtp-submission-input-for-owner-merge-smtp-bcc-recipients-parse-smtp-submission-sender
  - external/crate-pst-validate-uploaded-pst-file-with-validator
  - external/axum-body-body-http-method-request-statuscode
  - external/lpe-domain-smtpsubmissionrequest
  - external/lpe-magika-detectionsource-detector-magikadetection-validator
  - external/lpe-mail-auth-accountprincipal
  - external/lpe-storage-mail-parse-rfc822-message-submissionaccountidentity-submittedrecipientinput
  - external/std-fs-path-pathbuf-sync-mutex-mutexguard-time-systemtime-unix-epoch
  - external/tower-serviceext
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [router](../../../../functions/crates/lpe-admin-api/src/app/router.md)
- [protocol_router](../../../../functions/crates/lpe-admin-api/src/app/protocol_router.md)
- [init_observability](../../../../functions/crates/lpe-admin-api/src/app/init_observability.md)
- [observe_outbound_worker_poll](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll.md)
- [observe_outbound_worker_poll_failure](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll_failure.md)
- [observe_outbound_worker_dispatch](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_dispatch.md)
- [app_router_serves_exchange_mapi_options_route](../../../../functions/crates/lpe-admin-api/src/app/app_router_serves_exchange_mapi_options_route.md)
- [app_router_routes_mapi_post_to_exchange_handler](../../../../functions/crates/lpe-admin-api/src/app/app_router_routes_mapi_post_to_exchange_handler.md)
- [env_lock](../../../../functions/crates/lpe-admin-api/src/app/env_lock.md)
- [FakeDetector](../../../../classes/crates/lpe-admin-api/src/app/FakeDetector.md)
- [detect](../../../../functions/crates/lpe-admin-api/src/app/FakeDetector/detector/detect.md)
- [temp_file](../../../../functions/crates/lpe-admin-api/src/app/temp_file.md)
- [pst_upload_validation_accepts_valid_pst_like_file](../../../../functions/crates/lpe-admin-api/src/app/pst_upload_validation_accepts_valid_pst_like_file.md)
- [pst_upload_validation_rejects_extension_and_type_mismatch](../../../../functions/crates/lpe-admin-api/src/app/pst_upload_validation_rejects_extension_and_type_mismatch.md)
- [antispam_console_writes_are_lpe_ct_owned](../../../../functions/crates/lpe-admin-api/src/app/antispam_console_writes_are_lpe_ct_owned.md)
- [storage_policy_routes_are_registered](../../../../functions/crates/lpe-admin-api/src/app/storage_policy_routes_are_registered.md)
- [snapshot_routes_are_registered](../../../../functions/crates/lpe-admin-api/src/app/snapshot_routes_are_registered.md)
- [notes_journal_reminder_and_search_folder_routes_are_registered](../../../../functions/crates/lpe-admin-api/src/app/notes_journal_reminder_and_search_folder_routes_are_registered.md)
- [integration_secret_rejects_missing_or_weak_values](../../../../functions/crates/lpe-admin-api/src/app/integration_secret_rejects_missing_or_weak_values.md)
- [bootstrap_request_requires_explicit_strong_password](../../../../functions/crates/lpe-admin-api/src/app/bootstrap_request_requires_explicit_strong_password.md)
- [bootstrap_auto_request_requires_explicit_bootstrap_credentials](../../../../functions/crates/lpe-admin-api/src/app/bootstrap_auto_request_requires_explicit_bootstrap_credentials.md)
- [ha_role_check_accepts_only_active_role](../../../../functions/crates/lpe-admin-api/src/app/ha_role_check_accepts_only_active_role.md)
- [ha_active_work_follows_role_file](../../../../functions/crates/lpe-admin-api/src/app/ha_active_work_follows_role_file.md)
- [smtp_submission_derives_envelope_only_recipients_as_bcc](../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_derives_envelope_only_recipients_as_bcc.md)
- [smtp_submission_builds_canonical_submit_input](../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input.md)
- [smtp_submission_builds_send_as_input_for_delegated_mailbox](../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox.md)
- [smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox](../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox.md)

# Imports

- `axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, patch, post, put},
    Router,
}`
- `lpe_storage::Storage`
- `pub use crate::bootstrap::{
    bootstrap_admin, bootstrap_admin_request_from_env,
    bootstrap_admin_request_from_env_or_defaults, integration_shared_secret,
}`
- `pub use crate::readiness::{ha_allows_active_work, ha_current_role}`
- `crate::{
    admin_auth::{
        admin_auth_factors, enroll_totp, login, logout, me, oidc_callback, oidc_metadata,
        oidc_start, revoke_admin_factor, verify_totp_factor,
    },
    client_auth::{
        account_auth_factors, client_login, client_logout, client_me, client_oidc_callback,
        client_oidc_metadata, client_oidc_start, create_account_app_password,
        create_client_oauth_access_token, enroll_account_totp, list_account_app_passwords,
        revoke_account_app_password, revoke_account_factor, verify_account_totp_factor,
    },
    client_config,
    console::{
        attachment_support, create_account, create_alias, create_domain, create_filter_rule,
        create_mailbox, create_pst_transfer_job, create_server_administrator, dashboard,
        local_ai_health, mail_flow, run_pst_jobs, search_email_trace, update_account,
        update_antispam_settings, update_domain, update_local_ai_settings,
        update_security_settings, update_server_settings, upload_pst_import,
    },
    delegation::{
        delete_calendar_collection_grant, delete_collaboration_grant,
        delete_mailbox_delegation_grant, delete_sender_delegation_grant, delete_task_list_grant,
        get_free_busy, get_mailbox_delegation, list_collaboration_overview,
        upsert_calendar_collection_grant, upsert_collaboration_grant,
        upsert_mailbox_delegation_grant, upsert_sender_delegation_grant, upsert_task_list_grant,
    },
    health::{health, health_live, health_ready},
    integration::{
        accept_smtp_submission, authenticate_smtp_submission, deliver_inbound_message,
        verify_lpe_ct_recipient,
    },
    observability,
    pst::pst_upload_max_bytes,
    sieve::{
        delete_sieve_script, get_sieve_overview, get_sieve_script, list_mailbox_rules,
        put_sieve_script, rename_sieve_script, set_active_sieve_script,
    },
    snapshots::{create_snapshot, delete_snapshot, list_snapshots, restore_snapshot},
    storage::{
        create_storage_pool, get_storage_cleanup, get_storage_health, get_storage_migrations,
        get_storage_policies, list_storage_pools, update_account_storage_policy,
        update_domain_storage_policy, update_platform_storage_policy, update_storage_pool,
        update_tenant_storage_policy,
    },
    workspace::{
        client_workspace, create_public_folder_child, create_public_folder_tree,
        delete_client_contact, delete_client_event, delete_client_note, delete_client_task,
        delete_draft_message, delete_journal_entry, delete_public_folder,
        delete_public_folder_item, delete_public_folder_permission, delete_public_folder_replica,
        delete_search_folder, dismiss_recipient_suggestion, get_client_contact, get_client_note,
        get_client_task, get_journal_entry, get_public_folder, get_search_folder,
        list_client_contacts, list_client_notes, list_client_task_lists, list_client_tasks,
        list_contact_books, list_journal_entries, list_public_folder_children,
        list_public_folder_items, list_public_folder_per_user_state,
        list_public_folder_permissions, list_public_folder_replicas, list_public_folder_trees,
        list_recoverable_items, list_search_folders, outlook_profile_state, patch_client_contact,
        patch_public_folder_item, patch_public_folder_per_user_state, post_public_folder_item,
        purge_recoverable_item, put_public_folder_permission, put_public_folder_replica,
        query_client_reminders, query_recipient_suggestions, restore_recoverable_item,
        save_draft_message, submit_message, update_message_flag, update_public_folder,
        upsert_client_contact, upsert_client_event, upsert_client_note, upsert_client_task,
        upsert_journal_entry, upsert_search_folder,
    },
}`
- `super::{
        bootstrap_admin_request_from_env, bootstrap_admin_request_from_env_or_defaults,
        ha_allows_active_work, integration_shared_secret,
    }`
- `crate::ha_activation_check`
- `crate::integration::{
        build_smtp_submission_input_for_owner, merge_smtp_bcc_recipients,
        parse_smtp_submission_sender,
    }`
- `crate::pst::validate_uploaded_pst_file_with_validator`
- `axum::{
        body::Body,
        http::{Method, Request, StatusCode},
    }`
- `lpe_domain::SmtpSubmissionRequest`
- `lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator}`
- `lpe_mail_auth::AccountPrincipal`
- `lpe_storage::{
        mail::parse_rfc822_message, SubmissionAccountIdentity, SubmittedRecipientInput,
    }`
- `std::{
        fs,
        path::PathBuf,
        sync::{Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    }`
- `tower::ServiceExt`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
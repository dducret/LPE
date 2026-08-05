---
type: Rust Module
title: shared
resource: crates/lpe-storage/src/shared.rs#L1-L1081
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxdisplayname
  - external/serde-json-json-value
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-domain-from-email-normalize-domain-name-normalize-email-sha256-hex-auditentryinput-storage
  - external/std-time-systemtime-unix-epoch
  - external/crate-attachments-attachment-kind
  - external/crate-pst-validate-pst-import-path
  - external/crate-submission-normalize-bcc-recipients-normalize-visible-recipients-participants-normalized
  - external/crate-default-permissions-for-role-domain-from-email-normalize-admin-permissions-normalize-admin-session-auth-method-normalize-domain-name-normalize-email-normalize-task-status-submitmessageinput-submittedrecipientinput
  - external/lpe-magika-write-validation-record-expectedkind-ingresscontext-policydecision-validationoutcome-validationrequest
  - external/std-fs-time-systemtime-unix-epoch
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [fetch_account_category_modseq](../../../../functions/crates/lpe-storage/src/shared/Storage/fetch_account_category_modseq.md)
- [allocate_mail_modseq_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [ensure_account_exists](../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [ensure_mailbox](../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [insert_mail_change_log_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [affected_mail_principals_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [allocate_mailbox_membership_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [recalculate_mailbox_counts_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)
- [load_account_domain_id_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_message_blob_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [upsert_message_body_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [replace_message_headers_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)
- [upsert_mail_search_document_in_tx](../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx.md)
- [insert_audit](../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [tenant_id_for_domain_name](../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_name.md)
- [tenant_id_for_domain_id](../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_id.md)
- [tenant_id_for_account_id](../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [tenant_id_for_account_email](../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [tenant_id_for_admin_email](../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [allocate_uid_validity](../../../../functions/crates/lpe-storage/src/shared/allocate_uid_validity.md)
- [dedup_sorted_uuids](../../../../functions/crates/lpe-storage/src/shared/dedup_sorted_uuids.md)
- [submit_input](../../../../functions/crates/lpe-storage/src/shared/submit_input.md)
- [visible_recipients_exclude_bcc](../../../../functions/crates/lpe-storage/src/shared/visible_recipients_exclude_bcc.md)
- [bcc_recipients_are_kept_separately](../../../../functions/crates/lpe-storage/src/shared/bcc_recipients_are_kept_separately.md)
- [participants_normalized_ignores_bcc_addresses](../../../../functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses.md)
- [participants_normalized_remains_visible_only_even_with_bcc_display_name](../../../../functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name.md)
- [participants_normalized_allows_null_reverse_path](../../../../functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path.md)
- [pst_processing_requires_prior_validation_record](../../../../functions/crates/lpe-storage/src/shared/pst_processing_requires_prior_validation_record.md)
- [domain_dedup_scope_comes_from_account_email_domain](../../../../functions/crates/lpe-storage/src/shared/domain_dedup_scope_comes_from_account_email_domain.md)
- [mailbox_email_normalization_allows_eai_and_idna_domains](../../../../functions/crates/lpe-storage/src/shared/mailbox_email_normalization_allows_eai_and_idna_domains.md)
- [mailbox_domain_normalization_rejects_invalid_domains](../../../../functions/crates/lpe-storage/src/shared/mailbox_domain_normalization_rejects_invalid_domains.md)
- [task_status_defaults_to_needs_action](../../../../functions/crates/lpe-storage/src/shared/task_status_defaults_to_needs_action.md)
- [task_status_accepts_vtodo_aligned_values](../../../../functions/crates/lpe-storage/src/shared/task_status_accepts_vtodo_aligned_values.md)
- [task_status_rejects_unknown_values](../../../../functions/crates/lpe-storage/src/shared/task_status_rejects_unknown_values.md)
- [attachment_kind_falls_back_to_real_extension_label](../../../../functions/crates/lpe-storage/src/shared/attachment_kind_falls_back_to_real_extension_label.md)
- [built_in_role_permissions_include_dashboard](../../../../functions/crates/lpe-storage/src/shared/built_in_role_permissions_include_dashboard.md)
- [explicit_permissions_are_normalized_and_deduplicated](../../../../functions/crates/lpe-storage/src/shared/explicit_permissions_are_normalized_and_deduplicated.md)
- [admin_session_auth_method_collapses_totp_to_password_family](../../../../functions/crates/lpe-storage/src/shared/admin_session_auth_method_collapses_totp_to_password_family.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::MailboxDisplayName`
- `serde_json::{json, Value}`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    domain_from_email, normalize_domain_name, normalize_email, sha256_hex, AuditEntryInput, Storage,
}`
- `std::time::{SystemTime, UNIX_EPOCH}`
- `crate::attachments::attachment_kind`
- `crate::pst::validate_pst_import_path`
- `crate::submission::{
        normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    }`
- `crate::{
        default_permissions_for_role, domain_from_email, normalize_admin_permissions,
        normalize_admin_session_auth_method, normalize_domain_name, normalize_email,
        normalize_task_status, SubmitMessageInput, SubmittedRecipientInput,
    }`
- `lpe_magika::{
        write_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationOutcome,
        ValidationRequest,
    }`
- `std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    }`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)
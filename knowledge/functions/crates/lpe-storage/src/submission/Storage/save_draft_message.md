---
type: Rust Method
title: save_draft_message
resource: crates/lpe-storage/src/submission.rs#L245-L641
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/normalize_subject
  - functions/crates/lpe-storage/src/submission/types/normalize_visible_recipients
  - functions/crates/lpe-storage/src/submission/types/normalize_bcc_recipients
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/submission/Storage/ensure_same_tenant_account_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/resolve_submission_authorization_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox
  - functions/crates/lpe-storage/src/submission/types/participants_normalized
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn save_draft_message( &self, input: SubmitMessageInput, audit: AuditEntryInput, ) -> Result<SavedDraftMessage>`

# Calls

- [normalize_subject](../../../../../../functions/crates/lpe-storage/src/util/normalize_subject.md)
- [normalize_visible_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/normalize_visible_recipients.md)
- [normalize_bcc_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/normalize_bcc_recipients.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [ensure_same_tenant_account_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/ensure_same_tenant_account_in_tx.md)
- [resolve_submission_authorization_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/resolve_submission_authorization_in_tx.md)
- [ensure_mailbox](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [participants_normalized](../../../../../../functions/crates/lpe-storage/src/submission/types/participants_normalized.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_message_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [replace_message_headers_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx.md)
- [recalculate_mailbox_counts_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)
- [upsert_message_body_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)
- [upsert_mail_search_document_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
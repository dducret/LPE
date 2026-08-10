---
type: Rust Method
title: import_jmap_email
resource: crates/lpe-storage/src/message_ops.rs#L1095-L1318
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/submission/types/participants_normalized
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-storage/src/mail/parse_message_date_header
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/util/normalize_subject
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn import_jmap_email( &self, input: JmapImportedEmailInput, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [participants_normalized](../../../../../../functions/crates/lpe-storage/src/submission/types/participants_normalized.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_message_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [parse_message_date_header](../../../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [normalize_subject](../../../../../../functions/crates/lpe-storage/src/util/normalize_subject.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [replace_message_headers_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx.md)
- [upsert_message_body_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)
- [upsert_mail_search_document_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
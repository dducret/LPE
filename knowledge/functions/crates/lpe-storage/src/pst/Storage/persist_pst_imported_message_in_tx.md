---
type: Rust Method
title: persist_pst_imported_message_in_tx
resource: crates/lpe-storage/src/pst.rs#L383-L510
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst
---

# Signature

`async fn persist_pst_imported_message_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, job: &PendingPstJobRow, message: PstImportedMessage, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_message_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [upsert_message_body_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)
- [upsert_mail_search_document_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx.md)

# Called by

- [import_mailbox_from_pst](../../../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)
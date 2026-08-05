---
type: Rust Method
title: store_inbound_message_in_tx
resource: crates/lpe-storage/src/inbound.rs#L424-L565
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-storage/src/mail/parse_message_date_header
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn store_inbound_message_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, mailbox_id: Uuid, thread_id: Uuid, message_id: Uuid, request: &InboundDeliveryRequest, mail_from: &str, subject: &str, _preview: &str, size_octets: i64, body_text: &str, participants: &str, visible_recipients: &[(&'static str, SubmittedRecipientInput)], attachments: &[AttachmentUploadInput], ) -> Result<()>`

# Calls

- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_message_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [parse_message_date_header](../../../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [replace_message_headers_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx.md)
- [upsert_message_body_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)
- [upsert_mail_search_document_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_mail_search_document_in_tx.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
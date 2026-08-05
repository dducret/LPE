---
type: Rust Method
title: upsert_message_body_in_tx
resource: crates/lpe-storage/src/shared.rs#L472-L565
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn upsert_message_body_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, domain_id: Uuid, message_id: Uuid, body_text: &str, body_html_sanitized: Option<&str>, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)

# Called by

- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [update_jmap_email_content](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [insert_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
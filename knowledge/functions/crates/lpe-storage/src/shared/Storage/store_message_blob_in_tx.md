---
type: Rust Method
title: store_message_blob_in_tx
resource: crates/lpe-storage/src/shared.rs#L417-L470
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn store_message_blob_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, domain_id: Uuid, blob_kind: &str, media_type: &str, bytes: &[u8], ) -> Result<Uuid>`

# Calls

- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [insert_logical_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [insert_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
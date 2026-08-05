---
type: Rust Method
title: replace_message_headers_in_tx
resource: crates/lpe-storage/src/shared.rs#L567-L601
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mail/parse_header_records
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn replace_message_headers_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, message_id: Uuid, raw_message: &[u8], ) -> Result<usize>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [parse_header_records](../../../../../../functions/crates/lpe-storage/src/mail/parse_header_records.md)

# Called by

- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
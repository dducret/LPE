---
type: Rust Method
title: ensure_same_tenant_account_in_tx
resource: crates/lpe-storage/src/submission.rs#L1322-L1331
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
---

# Signature

`async fn ensure_same_tenant_account_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<()>`

# Calls

- [load_account_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)

# Called by

- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
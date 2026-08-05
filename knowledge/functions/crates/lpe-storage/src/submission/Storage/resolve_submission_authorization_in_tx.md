---
type: Rust Method
title: resolve_submission_authorization_in_tx
resource: crates/lpe-storage/src/submission.rs#L1284-L1383
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  - functions/crates/lpe-storage/src/util/trim_optional_text
  - functions/crates/lpe-storage/src/submission/Storage/has_sender_right_in_tx
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`async fn resolve_submission_authorization_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, input: &SubmitMessageInput, ) -> Result<ResolvedSubmissionAuthorization>`

# Calls

- [load_account_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [trim_optional_text](../../../../../../functions/crates/lpe-storage/src/util/trim_optional_text.md)
- [has_sender_right_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/has_sender_right_in_tx.md)

# Called by

- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
---
type: Rust Method
title: update_outbound_queue_status
resource: crates/lpe-storage/src/outbound.rs#L259-L412
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response
  - functions/crates/lpe-storage/src/outbound/normalize_handoff_response
  - functions/crates/lpe-storage/src/outbound/submission_queue_status
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  called_by:
  - functions/crates/lpe-cli/src/dispatch_outbound_message
  - functions/crates/lpe-storage/src/outbound/Storage/mark_outbound_queue_attempt_failure
---

# Signature

`pub async fn update_outbound_queue_status( &self, response: &OutboundMessageHandoffResponse, ) -> Result<OutboundQueueStatusUpdate>`

# Calls

- [should_ignore_handoff_response](../../../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)
- [normalize_handoff_response](../../../../../../functions/crates/lpe-storage/src/outbound/normalize_handoff_response.md)
- [submission_queue_status](../../../../../../functions/crates/lpe-storage/src/outbound/submission_queue_status.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)

# Called by

- [dispatch_outbound_message](../../../../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)
- [mark_outbound_queue_attempt_failure](../../../../../../functions/crates/lpe-storage/src/outbound/Storage/mark_outbound_queue_attempt_failure.md)
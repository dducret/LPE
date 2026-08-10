---
type: Rust Function
title: canonical_message_change_number_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L218-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_modseq_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
---

# Signature

`pub(crate) fn canonical_message_change_number_with_attachments( email: &JmapEmail, _attachments: &[AttachmentSyncFact], ) -> u64`

# Calls

- [canonical_modseq_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_modseq_change_number.md)

# Called by

- [normal_message_sync_facts_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max.md)
- [sync_state_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [email_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [canonical_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
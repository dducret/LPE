---
type: Rust Function
title: attachment_sync_fact_is_embedded_message
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1161-L1171
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
---

# Signature

`pub(crate) fn attachment_sync_fact_is_embedded_message(attachment: &AttachmentSyncFact) -> bool`

# Called by

- [sync_attachment_facts_for_with_embedded_content](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/sync_attachment_facts_for_with_embedded_content.md)
- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)
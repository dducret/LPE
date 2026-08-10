---
type: Rust Function
title: canonical_modseq_change_number
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1254-L1256
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
---

# Signature

`fn canonical_modseq_change_number(modseq: u64) -> u64`

# Called by

- [canonical_folder_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number.md)
- [canonical_message_change_number_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)
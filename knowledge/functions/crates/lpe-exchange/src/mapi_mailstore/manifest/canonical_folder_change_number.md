---
type: Rust Function
title: canonical_folder_change_number
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L209-L211
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_modseq_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_change_numbers_fit_mapi_globcnt
---

# Signature

`pub(crate) fn canonical_folder_change_number(mailbox: &JmapMailbox) -> u64`

# Calls

- [canonical_modseq_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_modseq_change_number.md)

# Called by

- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [canonical_hierarchy_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [canonical_change_numbers_fit_mapi_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_change_numbers_fit_mapi_globcnt.md)
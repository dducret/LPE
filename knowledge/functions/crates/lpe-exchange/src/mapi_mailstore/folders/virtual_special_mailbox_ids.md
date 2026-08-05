---
type: Rust Function
title: virtual_special_mailbox_ids
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L307-L315
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/repair_stale_mapi_object_identities
---

# Signature

`pub(crate) fn virtual_special_mailbox_ids() -> impl Iterator<Item = Uuid>`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)

# Called by

- [repair_stale_mapi_object_identities](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/repair_stale_mapi_object_identities.md)
---
type: Rust Function
title: public_folder_item_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L410-L452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`fn public_folder_item_sync_object( item: &crate::mapi_store::MapiPublicFolderItem, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
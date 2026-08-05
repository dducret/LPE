---
type: Rust Function
title: serialize_advertised_special_folder_row_with_counts
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L310-L327
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid
---

# Signature

`pub(super) fn serialize_advertised_special_folder_row_with_counts( folder_id: u64, columns: &[u32], mailbox_guid: Uuid, content_count: u32, unread_count: u32, deleted_count: u32, ) -> Vec<u8>`

# Calls

- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [serialize_advertised_special_folder_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid.md)
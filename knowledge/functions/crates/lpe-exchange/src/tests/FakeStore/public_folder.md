---
type: Rust Method
title: public_folder
resource: crates/lpe-exchange/src/tests/mod.rs#L4456-L4477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/update_item_rejects_public_folder_item_without_write_access
  - functions/crates/lpe-exchange/src/tests/ews/delete_item_rejects_public_folder_item_without_delete_access
  - functions/crates/lpe-exchange/src/tests/ews/create_item_rejects_public_folder_post_without_write_access
  - functions/crates/lpe-exchange/src/tests/ews/find_item_rejects_public_folder_without_read_access
  - functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_rejects_public_folder_without_read_access
  - functions/crates/lpe-exchange/src/tests/ews/get_item_rejects_public_folder_item_without_read_access
  - functions/crates/lpe-exchange/src/tests/ews/move_item_rejects_public_folder_target_without_write_access
  - functions/crates/lpe-exchange/src/tests/ews/copy_item_rejects_public_folder_target_without_write_access
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_requires_share_right
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_public_folder_child
---

# Signature

`fn public_folder(id: &str, parent_id: Option<&str>, display_name: &str) -> PublicFolder`

# Called by

- [update_item_rejects_public_folder_item_without_write_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/update_item_rejects_public_folder_item_without_write_access.md)
- [delete_item_rejects_public_folder_item_without_delete_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/delete_item_rejects_public_folder_item_without_delete_access.md)
- [create_item_rejects_public_folder_post_without_write_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_item_rejects_public_folder_post_without_write_access.md)
- [find_item_rejects_public_folder_without_read_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/find_item_rejects_public_folder_without_read_access.md)
- [sync_folder_items_rejects_public_folder_without_read_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/sync_folder_items_rejects_public_folder_without_read_access.md)
- [get_item_rejects_public_folder_item_without_read_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/get_item_rejects_public_folder_item_without_read_access.md)
- [move_item_rejects_public_folder_target_without_write_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/move_item_rejects_public_folder_target_without_write_access.md)
- [copy_item_rejects_public_folder_target_without_write_access](../../../../../../functions/crates/lpe-exchange/src/tests/ews/copy_item_rejects_public_folder_target_without_write_access.md)
- [mapi_over_http_public_folder_modify_permissions_requires_share_right](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_requires_share_right.md)
- [create_public_folder_child](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_public_folder_child.md)
---
type: Rust Function
title: test_mapi_pcl_includes_change_key
resource: crates/lpe-exchange/src/tests/mod.rs#L4723-L4756
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_jmap_email_from_mailbox_with_mapi_identity
---

# Signature

`fn test_mapi_pcl_includes_change_key(predecessor_change_list: &[u8], change_key: &[u8]) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_mapi_folder_hierarchy_change](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [create_mapi_contact](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update.md)
- [move_accessible_event_to_deleted_items](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items.md)
- [commit_mapi_navigation_shortcut_import](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [commit_mapi_associated_config_import](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import.md)
- [move_jmap_email_from_mailbox_with_mapi_identity](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_jmap_email_from_mailbox_with_mapi_identity.md)
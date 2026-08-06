---
type: Rust Function
title: test_merge_mapi_predecessor_change_lists
resource: crates/lpe-exchange/src/tests/mod.rs#L4827-L4862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update
---

# Signature

`fn test_merge_mapi_predecessor_change_lists(first: &[u8], second: &[u8]) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [mapi_navigation_shortcut_import_commits_content_and_identity_atomically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)
- [commit_mapi_folder_hierarchy_change](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [create_mapi_contact](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact.md)
- [commit_mapi_navigation_shortcut_update](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update.md)
- [commit_mapi_navigation_shortcut_import](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [commit_mapi_associated_config_update](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update.md)
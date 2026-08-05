---
type: Rust Method
title: commit_mapi_navigation_shortcut_update
resource: crates/lpe-exchange/src/tests/mod.rs#L9686-L9763
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
---

# Signature

`fn commit_mapi_navigation_shortcut_update<'a>( &'a self, input: crate::store::UpsertMapiNavigationShortcutInput, ) -> StoreFuture<'a, crate::store::MapiNavigationShortcutCommit>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [upsert_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut.md)

# Called by

- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
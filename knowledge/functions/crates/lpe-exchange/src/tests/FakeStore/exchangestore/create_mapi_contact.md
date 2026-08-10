---
type: Rust Method
title: create_mapi_contact
resource: crates/lpe-exchange/src/tests/mod.rs#L8142-L8525
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_mapi_contact<'a>( &'a self, input: MapiContactCreateInput, ) -> StoreFuture<'a, MapiContactCreateOutcome>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
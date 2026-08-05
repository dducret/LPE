---
type: Rust Function
title: predecessor_merge_is_idempotent_and_keeps_both_replicas
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L329-L345
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list
---

# Signature

`fn predecessor_merge_is_idempotent_and_keeps_both_replicas()`

# Calls

- [parse_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list.md)
- [merge_mapi_predecessor_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key.md)
- [serialize_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list.md)
---
type: Rust Function
title: imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L170-L194
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/realistic_imported_identity
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
---

# Signature

`fn imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key()`

# Calls

- [realistic_imported_identity](../../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/realistic_imported_identity.md)
- [mapi_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
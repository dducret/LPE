---
type: Rust Function
title: pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas
resource: crates/lpe-storage/src/mapi_events.rs#L1514-L1532
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
---

# Signature

`fn pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas()`

# Calls

- [mapi_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
---
type: Rust Method
title: local_replica_deleted_ranges
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L551-L590
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response
---

# Signature

`pub(in crate::mapi) fn local_replica_deleted_ranges( &self, ) -> Option<Vec<MapiLocalReplicaDeletedRange>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_globcnt](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_local_replica_midset_deleted_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response.md)
---
type: Rust Method
title: local_replica_id_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L676-L682
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response
---

# Signature

`pub(in crate::mapi) fn local_replica_id_count(&self) -> u32`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_local_replica_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response.md)
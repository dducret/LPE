---
type: Rust Function
title: source_key_replica_counter
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L310-L314
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn source_key_replica_counter(source_key: &[u8]) -> Option<([u8; 16], u64)>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [replguid_idset_from_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
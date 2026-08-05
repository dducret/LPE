---
type: Rust Method
title: local
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L291-L294
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn local(&self) -> Option<&CounterSet>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
---
type: Rust Method
title: local_mut
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L296-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn local_mut(&mut self) -> &mut CounterSet`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
---
type: Rust Function
title: write_state
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1118-L1146
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn write_state(output: &mut Vec<u8>, sync_type: u8, state: &SyncStateSets)`

# Calls

- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [encode_replguid_sets](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/encode_replguid_sets.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
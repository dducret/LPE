---
type: Rust Function
title: write_deletion_section
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1089-L1106
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn write_deletion_section( output: &mut Vec<u8>, deleted: &CounterSet, no_longer_in_scope: &CounterSet, expired: &CounterSet, )`

# Calls

- [write_replid_idset_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
---
type: Rust Function
title: parse_standalone_state
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L666-L676
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn parse_standalone_state( bytes: &[u8], sync_type: u8, label: &str, ) -> Result<SyncStateSets, String>`

# Calls

- [parse_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
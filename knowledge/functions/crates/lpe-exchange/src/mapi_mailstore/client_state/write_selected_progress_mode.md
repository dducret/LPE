---
type: Rust Function
title: write_selected_progress_mode
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L980-L1012
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn write_selected_progress_mode( output: &mut Vec<u8>, prefix: [u8; 4], retained: &[&ManifestChange], ) -> Result<(), String>`

# Calls

- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
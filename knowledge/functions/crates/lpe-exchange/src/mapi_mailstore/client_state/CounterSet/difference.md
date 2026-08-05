---
type: Rust Method
title: difference
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L237-L263
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference
---

# Signature

`fn difference(&self, other: &Self) -> Self`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [counter_difference](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference.md)
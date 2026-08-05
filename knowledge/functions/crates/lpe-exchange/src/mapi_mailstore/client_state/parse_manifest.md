---
type: Rust Function
title: parse_manifest
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L511-L604
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_per_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
---

# Signature

`fn parse_manifest(bytes: &[u8], sync_type: u8, sync_flags: u16) -> Result<ParsedManifest, String>`

# Calls

- [parse_progress_mode](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_mode.md)
- [parse_progress_per_message](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_per_message.md)
- [parse_change](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section.md)
- [union_with](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with.md)
- [parse_read_state_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section.md)
- [parse_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
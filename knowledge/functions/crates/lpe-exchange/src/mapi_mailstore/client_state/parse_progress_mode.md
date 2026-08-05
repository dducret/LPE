---
type: Rust Function
title: parse_progress_mode
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L944-L953
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
---

# Signature

`fn parse_progress_mode(bytes: &[u8], start: usize) -> Result<([u8; 4], usize), String>`

# Calls

- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)

# Called by

- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
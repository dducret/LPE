---
type: Rust Function
title: parse_progress_per_message
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L955-L978
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

`fn parse_progress_per_message( bytes: &[u8], start: usize, ) -> Result<(ProgressPerMessage, usize), String>`

# Calls

- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)

# Called by

- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
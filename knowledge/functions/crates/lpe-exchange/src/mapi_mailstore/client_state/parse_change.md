---
type: Rust Function
title: parse_change
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L606-L664
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_change_boundary
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_fast_transfer_marker
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
---

# Signature

`fn parse_change(bytes: &[u8], start: usize, sync_type: u8) -> Result<ManifestChange, String>`

# Calls

- [is_change_boundary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_change_boundary.md)
- [is_fast_transfer_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_fast_transfer_marker.md)
- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)

# Called by

- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
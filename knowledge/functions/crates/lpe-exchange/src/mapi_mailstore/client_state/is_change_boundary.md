---
type: Rust Function
title: is_change_boundary
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1256-L1266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section
---

# Signature

`fn is_change_boundary(tag: u32) -> bool`

# Called by

- [parse_change](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change.md)
- [parse_read_state_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section.md)
- [parse_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section.md)
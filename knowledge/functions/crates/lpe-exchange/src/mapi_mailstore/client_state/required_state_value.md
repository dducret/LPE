---
type: Rust Function
title: required_state_value
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L759-L765
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state
---

# Signature

`fn required_state_value<'a>( value: Option<&'a [u8]>, tag: u32, label: &str, ) -> Result<&'a [u8], String>`

# Called by

- [parse_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)
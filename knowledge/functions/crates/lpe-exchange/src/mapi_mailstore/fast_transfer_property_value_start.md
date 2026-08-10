---
type: Rust Function
title: fast_transfer_property_value_start
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1379-L1421
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property
  - functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property
---

# Signature

`pub(crate) fn fast_transfer_property_value_start( bytes: &[u8], property_tag: u32, offset_after_tag: usize, ) -> Result<usize, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)
- [parse_debug_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)
- [strict_parse_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property.md)
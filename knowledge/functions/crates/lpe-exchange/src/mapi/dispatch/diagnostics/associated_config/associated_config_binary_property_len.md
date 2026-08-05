---
type: Rust Function
title: associated_config_binary_property_len
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config.rs#L60-L68
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_open_shape
---

# Signature

`fn associated_config_binary_property_len( message: &crate::mapi_store::MapiAssociatedConfigMessage, property_tag: u32, ) -> Option<usize>`

# Calls

- [associated_config_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)

# Called by

- [associated_config_open_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_open_shape.md)
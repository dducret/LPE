---
type: Rust Function
title: parse_property
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1160-L1195
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_fast_transfer_marker
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/variable_property_range
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/fixed_property_range
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/multi_string_property_range
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_per_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section
---

# Signature

`fn parse_property(bytes: &[u8], offset: usize) -> Result<ParsedProperty<'_>, String>`

# Calls

- [is_fast_transfer_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_fast_transfer_marker.md)
- [fast_transfer_property_value_start](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start.md)
- [variable_property_range](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/variable_property_range.md)
- [fixed_property_range](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/fixed_property_range.md)
- [multi_string_property_range](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/multi_string_property_range.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_change](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change.md)
- [parse_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)
- [parse_progress_mode](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_mode.md)
- [parse_progress_per_message](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_progress_per_message.md)
- [parse_read_state_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section.md)
- [parse_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section.md)
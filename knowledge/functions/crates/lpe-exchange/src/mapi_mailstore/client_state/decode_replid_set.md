---
type: Rust Function
title: decode_replid_set
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L792-L819
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section
---

# Signature

`fn decode_replid_set(value: &[u8]) -> Result<CounterSet, String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [decode_globset_range_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix.md)
- [union_with](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with.md)
- [from_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges.md)

# Called by

- [parse_read_state_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_read_state_section.md)
- [parse_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_deletion_section.md)
---
type: Rust Function
title: decode_replguid_set
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L767-L790
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/validate_download_state_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state
---

# Signature

`fn decode_replguid_set(value: &[u8]) -> Result<ReplicaCounterSets, String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [decode_globset_range_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix.md)
- [from_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges.md)

# Called by

- [validate_download_state_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/validate_download_state_property.md)
- [parse_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_state.md)
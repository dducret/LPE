---
type: Rust Method
title: union_with
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L230-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set
---

# Signature

`fn union_with(&mut self, other: &Self)`

# Calls

- [from_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/from_ranges.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [select_download_manifest_for_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [parse_manifest](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
- [decode_replid_set](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set.md)
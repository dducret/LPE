---
type: Rust Function
title: source_key_for_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L1019-L1026
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id
---

# Signature

`pub(crate) fn source_key_for_object_id(object_id: u64) -> Vec<u8>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [raw_source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id.md)
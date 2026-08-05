---
type: Rust Function
title: instance_key_for_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L1039-L1046
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/raw_instance_key_for_object_id
---

# Signature

`pub(crate) fn instance_key_for_object_id(object_id: u64) -> Vec<u8>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [raw_instance_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_instance_key_for_object_id.md)
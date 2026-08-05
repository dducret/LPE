---
type: Rust Function
title: durable_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L57-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids
---

# Signature

`pub(crate) fn durable_object_id(object_id: u64) -> Option<u64>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [actual_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)

# Called by

- [replguid_idset_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids.md)
- [replid_idset_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)
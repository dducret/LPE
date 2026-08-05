---
type: Rust Method
title: from_identity_records
resource: crates/lpe-exchange/src/mapi_store/folder_versions.rs#L10-L35
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`pub(super) fn from_identity_records( records: &[MapiIdentityRecord], identity_codec: &crate::mapi::identity::MapiIdentityCodec, ) -> Self`

# Calls

- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
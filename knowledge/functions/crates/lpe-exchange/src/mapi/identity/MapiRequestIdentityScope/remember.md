---
type: Rust Method
title: remember
resource: crates/lpe-exchange/src/mapi/identity.rs#L107-L118
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
---

# Signature

`fn remember(&self, canonical_id: Uuid, object_id: u64, source_key: Option<Vec<u8>>)`

# Called by

- [seed_from_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
---
type: Rust Method
title: fetch_mapi_identities_by_source_keys
resource: crates/lpe-exchange/src/tests/mod.rs#L6758-L6777
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip
---

# Signature

`fn fetch_mapi_identities_by_source_keys<'a>( &'a self, _account_id: Uuid, source_keys: &'a [Vec<u8>], ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fake_mapi_identity_lookup_for_object_id](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id.md)

# Called by

- [mapi_identity_source_key_lookup_and_checkpoints_round_trip](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip.md)
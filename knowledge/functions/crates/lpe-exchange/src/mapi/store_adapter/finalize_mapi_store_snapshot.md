---
type: Rust Function
title: finalize_mapi_store_snapshot
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L900-L911
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/durable_identity_records
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn finalize_mapi_store_snapshot( snapshot: MapiMailStoreSnapshot, identity_scope: &MapiIdentityScope, request_identity_scope: &crate::mapi::identity::MapiRequestIdentityScope, ) -> MapiMailStoreSnapshot`

# Calls

- [with_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_identity_codec.md)
- [seed_from_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [durable_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/durable_identity_records.md)
- [identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
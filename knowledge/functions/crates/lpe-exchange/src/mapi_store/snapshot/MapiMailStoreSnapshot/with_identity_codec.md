---
type: Rust Method
title: with_identity_codec
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L83-L89
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_identity_codec( mut self, identity_codec: crate::mapi::identity::MapiIdentityCodec, ) -> Self`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [finalize_mapi_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
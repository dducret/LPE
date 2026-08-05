---
type: Rust Method
title: is_special_canonical_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L267-L269
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn is_special_canonical_id(&self, canonical_id: &Uuid) -> bool`

# Called by

- [seed_from_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [load_mapi_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
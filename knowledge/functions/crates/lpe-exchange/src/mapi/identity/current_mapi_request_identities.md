---
type: Rust Function
title: current_mapi_request_identities
resource: crates/lpe-exchange/src/mapi/identity.rs#L47-L51
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key
---

# Signature

`fn current_mapi_request_identities<T>( mapper: impl FnOnce(&MapiRequestIdentityScope) -> T, ) -> Option<T>`

# Called by

- [remember_mapi_identity_with_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [forget_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapped_mapi_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key.md)
---
type: Rust Function
title: request_scope_keeps_special_folder_parent_identity_logical_and_durable
resource: crates/lpe-exchange/src/mapi/identity.rs#L1414-L1490
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
---

# Signature

`async fn request_scope_keeps_special_folder_parent_identity_logical_and_durable()`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [with_current_mapi_request_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope.md)
- [with_current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [serialize_folder_row_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [source_key_for_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
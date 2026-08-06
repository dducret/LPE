---
type: Rust Function
title: dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L5959-L6019
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_contact_folder_associated_find_row_does_not_return_config
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
---

# Signature

`fn dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [collaboration_folder_identity_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [assert_contact_folder_associated_find_row_does_not_return_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_contact_folder_associated_find_row_does_not_return_config.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
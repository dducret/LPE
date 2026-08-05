---
type: Rust Method
title: fetch_accessible_contacts_internal
resource: crates/lpe-storage/src/collaboration.rs#L1194-L1336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
  - functions/crates/lpe-storage/src/collaboration/types/contact_book_role_for_collection_id
  - functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_by_ids
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_in_collection
---

# Signature

`async fn fetch_accessible_contacts_internal( &self, principal_account_id: Uuid, collection_id: Option<&str>, ids: Option<&[Uuid]>, ) -> Result<Vec<AccessibleContact>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [contact_book_role_for_collection_id](../../../../../../functions/crates/lpe-storage/src/collaboration/types/contact_book_role_for_collection_id.md)
- [collection_id_for_owner](../../../../../../functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner.md)

# Called by

- [fetch_accessible_contacts](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts.md)
- [fetch_accessible_contacts_by_ids](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_by_ids.md)
- [fetch_accessible_contacts_in_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_in_collection.md)
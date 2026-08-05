---
type: Rust Function
title: contact_book_role_for_collection_id
resource: crates/lpe-storage/src/collaboration/types.rs#L367-L374
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_contact
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal
---

# Signature

`pub(super) fn contact_book_role_for_collection_id(collection_id: Option<&str>) -> &'static str`

# Called by

- [create_accessible_contact](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact.md)
- [update_accessible_contact](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_contact.md)
- [fetch_accessible_contacts_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal.md)
---
type: Rust Method
title: update_accessible_contact
resource: crates/lpe-storage/src/collaboration.rs#L592-L640
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/collaboration/types/contact_book_role_for_collection_id
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`pub async fn update_accessible_contact( &self, principal_account_id: Uuid, contact_id: Uuid, input: UpsertClientContactInput, ) -> Result<AccessibleContact>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [contact_book_role_for_collection_id](../../../../../../functions/crates/lpe-storage/src/collaboration/types/contact_book_role_for_collection_id.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
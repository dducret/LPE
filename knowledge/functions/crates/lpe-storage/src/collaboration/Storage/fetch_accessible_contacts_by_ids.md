---
type: Rust Method
title: fetch_accessible_contacts_by_ids
resource: crates/lpe-storage/src/collaboration.rs#L519-L529
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal
---

# Signature

`pub async fn fetch_accessible_contacts_by_ids( &self, principal_account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<AccessibleContact>>`

# Calls

- [fetch_accessible_contacts_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal.md)
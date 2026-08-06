---
type: Rust Function
title: delete_custom_properties_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1604-L1636
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
---

# Signature

`async fn delete_custom_properties_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, contact_id: Uuid, property_tags: &[u32], ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
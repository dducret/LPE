---
type: Rust Function
title: fetch_principal_contact_identity_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1664-L1705
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
---

# Signature

`async fn fetch_principal_contact_identity_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, contact_id: Uuid, ) -> Result<AllocatedContactIdentity>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
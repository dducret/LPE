---
type: Rust Function
title: repair_stale_mapi_object_identities
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L518-L810
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_ids
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn repair_stale_mapi_object_identities( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, preserved_mailbox_identity_ids: &[Uuid], ) -> Result<()>`

# Calls

- [virtual_special_mailbox_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_ids.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
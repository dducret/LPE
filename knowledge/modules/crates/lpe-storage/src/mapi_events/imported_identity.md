---
type: Rust Module
title: imported_identity
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L1-L207
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-postgres
  - external/uuid-uuid
  - external/super-mapi-change-key-mapi-store-id-merge-predecessor-change-list-eventidentityversion-mapieventimportedidentity-first-dynamic-mapi-global-counter-first-reserved-high-global-counter
  - external/crate-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-ensure-mapi-store-identity-in-tx
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [validate_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/validate_imported_identity.md)
- [imported_source_global_counter](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_source_global_counter.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [realistic_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/realistic_imported_identity.md)
- [imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key.md)
- [imported_identity_rejects_a_foreign_source_key_replica](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_rejects_a_foreign_source_key_replica.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::Postgres`
- `uuid::Uuid`
- `super::{
    mapi_change_key, mapi_store_id, merge_predecessor_change_list, EventIdentityVersion,
    MapiEventImportedIdentity, FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER,
    FIRST_RESERVED_HIGH_GLOBAL_COUNTER,
}`
- `crate::mapi_store_identity::{
    allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
    ensure_mapi_store_identity_in_tx,
}`
- `super::*`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)
---
type: Rust Module
title: deleted_events
resource: crates/lpe-storage/src/collaboration/deleted_events.rs#L1-L454
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-mapi-events-mapi-change-key-mapi-store-id-merge-predecessor-change-list-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-ensure-mapi-store-identity-in-tx-mapi-first-global-counter-mapi-first-reserved-high-global-counter-mapi-max-global-counter-canonicalchangecategory-storage
  - external/super-accessibleevent-mapieventidentitymove-mapieventimportedmoveidentity-moveaccessibleeventtodeleteditemsresult
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [fetch_accessible_deleted_events](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/fetch_accessible_deleted_events.md)
- [move_accessible_event_to_deleted_items](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items.md)
- [rekey_active_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx.md)
- [imported_move_destination_global_counter](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/imported_move_destination_global_counter.md)
- [allocate_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx.md)
- [checked_positive_u64](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/checked_positive_u64.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    mapi_events::{mapi_change_key, mapi_store_id, merge_predecessor_change_list},
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
        ensure_mapi_store_identity_in_tx, MAPI_FIRST_GLOBAL_COUNTER,
        MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
    CanonicalChangeCategory, Storage,
}`
- `super::{
    AccessibleEvent, MapiEventIdentityMove, MapiEventImportedMoveIdentity,
    MoveAccessibleEventToDeletedItemsResult,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)
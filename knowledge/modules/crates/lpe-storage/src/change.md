---
type: Rust Module
title: change
resource: crates/lpe-storage/src/change.rs#L1-L795
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/sqlx-postgres-pglistener-postgres
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-collaborationresourcekind-storage-canonical-change-channel
  - external/super-cursor-is-before-retained-floor-canonicalchangecategory
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [CanonicalChangeCategory](../../../../classes/crates/lpe-storage/src/change/CanonicalChangeCategory.md)
- [as_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/as_str.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [CanonicalChangeListener](../../../../classes/crates/lpe-storage/src/change/CanonicalChangeListener.md)
- [CanonicalPushChangeSet](../../../../classes/crates/lpe-storage/src/change/CanonicalPushChangeSet.md)
- [CanonicalChangeReplay](../../../../classes/crates/lpe-storage/src/change/CanonicalChangeReplay.md)
- [is_empty](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/is_empty.md)
- [insert_accounts](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [accounts_for](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/accounts_for.md)
- [contains_category](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/contains_category.md)
- [set_journal_cursor](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor.md)
- [journal_cursor](../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/journal_cursor.md)
- [wait_for_change](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change.md)
- [CanonicalChangeNotification](../../../../classes/crates/lpe-storage/src/change/CanonicalChangeNotification.md)
- [create_canonical_change_listener](../../../../functions/crates/lpe-storage/src/change/Storage/create_canonical_change_listener.md)
- [emit_canonical_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)
- [emit_mail_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [emit_mail_delegation_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change.md)
- [emit_collaboration_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [emit_collaboration_grant_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change.md)
- [emit_task_access_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)
- [emit_account_scoped_change](../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [insert_collaboration_tombstone_in_tx](../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [insert_collaboration_move_tombstone_in_tx](../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_move_tombstone_in_tx.md)
- [insert_collaboration_tombstone_with_reason_in_tx](../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx.md)
- [fetch_canonical_change_cursor](../../../../functions/crates/lpe-storage/src/change/Storage/fetch_canonical_change_cursor.md)
- [replay_canonical_changes](../../../../functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes.md)
- [purge_canonical_change_journals](../../../../functions/crates/lpe-storage/src/change/Storage/purge_canonical_change_journals.md)
- [purge_expired_replay_rows](../../../../functions/crates/lpe-storage/src/change/Storage/purge_expired_replay_rows.md)
- [CanonicalChangeJournalRow](../../../../classes/crates/lpe-storage/src/change/CanonicalChangeJournalRow.md)
- [dedup_sorted_uuids](../../../../functions/crates/lpe-storage/src/change/dedup_sorted_uuids.md)
- [cursor_is_before_retained_floor](../../../../functions/crates/lpe-storage/src/change/cursor_is_before_retained_floor.md)
- [rights_change_category_round_trips](../../../../functions/crates/lpe-storage/src/change/rights_change_category_round_trips.md)
- [retained_floor_detects_stale_cursor_with_newer_journal](../../../../functions/crates/lpe-storage/src/change/retained_floor_detects_stale_cursor_with_newer_journal.md)
- [retained_floor_accepts_current_retained_cursor](../../../../functions/crates/lpe-storage/src/change/retained_floor_accepts_current_retained_cursor.md)
- [retained_floor_ignores_empty_journal](../../../../functions/crates/lpe-storage/src/change/retained_floor_ignores_empty_journal.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `sqlx::{postgres::PgListener, Postgres}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{CollaborationResourceKind, Storage, CANONICAL_CHANGE_CHANNEL}`
- `super::{cursor_is_before_retained_floor, CanonicalChangeCategory}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)
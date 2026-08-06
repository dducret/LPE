---
type: Rust Function
title: storage_jmap_fixture
resource: crates/lpe-jmap/src/tests.rs#L8697-L8887
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-jmap/src/tests/storage_backed_jmap_import_copy_get_changes_and_query_changes_round_trip
  - functions/crates/lpe-jmap/src/tests/storage_backed_calendar_event_lifecycle_updates_canonical_views
  - functions/crates/lpe-jmap/src/tests/storage_backed_private_share_reminder_and_durable_change_round_trip
---

# Signature

`async fn storage_jmap_fixture() -> Result<Option<StorageJmapFixture>>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [storage_backed_jmap_import_copy_get_changes_and_query_changes_round_trip](../../../../../functions/crates/lpe-jmap/src/tests/storage_backed_jmap_import_copy_get_changes_and_query_changes_round_trip.md)
- [storage_backed_calendar_event_lifecycle_updates_canonical_views](../../../../../functions/crates/lpe-jmap/src/tests/storage_backed_calendar_event_lifecycle_updates_canonical_views.md)
- [storage_backed_private_share_reminder_and_durable_change_round_trip](../../../../../functions/crates/lpe-jmap/src/tests/storage_backed_private_share_reminder_and_durable_change_round_trip.md)
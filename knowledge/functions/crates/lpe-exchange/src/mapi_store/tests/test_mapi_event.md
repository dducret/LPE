---
type: Rust Function
title: test_mapi_event
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L143-L163
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/tests/test_accessible_event
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/tests/event_lookup_rejects_another_principals_cached_mid
  - functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias
---

# Signature

`fn test_mapi_event(canonical_id: Uuid, account_id: Uuid, object_id: u64, title: &str) -> MapiEvent`

# Calls

- [test_accessible_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_accessible_event.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [event_lookup_rejects_another_principals_cached_mid](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/event_lookup_rejects_another_principals_cached_mid.md)
- [exact_event_mid_wins_over_another_events_foreign_cached_alias](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias.md)
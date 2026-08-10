---
type: Rust Function
title: calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L2402-L2782
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity( ) -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
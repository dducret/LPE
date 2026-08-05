---
type: Rust Function
title: mapi_event_fixture_drop_cleans_temporary_schema
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L436-L463
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
---

# Signature

`async fn mapi_event_fixture_drop_cleans_temporary_schema() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
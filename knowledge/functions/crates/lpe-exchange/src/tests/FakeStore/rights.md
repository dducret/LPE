---
type: Rust Method
title: rights
resource: crates/lpe-exchange/src/tests/mod.rs#L4210-L4217
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/collection
  - functions/crates/lpe-exchange/src/tests/FakeStore/contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/task
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_event
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task
---

# Signature

`fn rights() -> CollaborationRights`

# Called by

- [mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_sort_normalizes_dynamic_named_property_ids.md)
- [collection](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/collection.md)
- [contact](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/contact.md)
- [task](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/task.md)
- [create_accessible_contact](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_contact.md)
- [create_accessible_event](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_event.md)
- [create_accessible_task](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task.md)
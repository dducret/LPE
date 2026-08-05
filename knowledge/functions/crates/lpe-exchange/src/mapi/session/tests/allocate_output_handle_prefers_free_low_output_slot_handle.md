---
type: Rust Function
title: allocate_output_handle_prefers_free_low_output_slot_handle
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L758-L774
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
---

# Signature

`fn allocate_output_handle_prefers_free_low_output_slot_handle()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
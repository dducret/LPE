---
type: Rust Function
title: read_recipients_row_zero_on_empty_message_returns_not_found
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L340-L362
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
---

# Signature

`fn read_recipients_row_zero_on_empty_message_returns_not_found()`

# Calls

- [rop_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
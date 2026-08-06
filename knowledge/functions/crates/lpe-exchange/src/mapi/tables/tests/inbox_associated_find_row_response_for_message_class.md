---
type: Rust Function
title: inbox_associated_find_row_response_for_message_class
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8597-L8630
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_virtual_rule_organizer
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_inbox_associated_find_row_no_match_for_message_class
---

# Signature

`fn inbox_associated_find_row_response_for_message_class(message_class: &str) -> Vec<u8>`

# Calls

- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)

# Called by

- [inbox_associated_find_row_returns_virtual_rule_organizer](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_virtual_rule_organizer.md)
- [assert_inbox_associated_find_row_no_match_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_inbox_associated_find_row_no_match_for_message_class.md)
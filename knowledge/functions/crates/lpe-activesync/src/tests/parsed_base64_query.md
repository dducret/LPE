---
type: Rust Function
title: parsed_base64_query
resource: crates/lpe-activesync/src/tests.rs#L1920-L1927
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query
  - functions/crates/lpe-activesync/src/tests/base64_query
  called_by:
  - functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches
---

# Signature

`fn parsed_base64_query(command_code: u8, device_id: &str) -> ParsedActiveSyncQuery`

# Calls

- [from_raw_query](../../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)
- [base64_query](../../../../../functions/crates/lpe-activesync/src/tests/base64_query.md)

# Called by

- [base64_sync_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches.md)
- [base64_ping_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches.md)
- [base64_send_mail_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches.md)
- [base64_smart_reply_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches.md)
- [base64_smart_forward_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches.md)
- [base64_move_items_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches.md)
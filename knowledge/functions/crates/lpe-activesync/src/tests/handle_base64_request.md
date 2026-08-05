---
type: Rust Function
title: handle_base64_request
resource: crates/lpe-activesync/src/tests.rs#L1928-L1938
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
  called_by:
  - functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches
  - functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches
---

# Signature

`async fn handle_base64_request( service: &ActiveSyncService<FakeStore>, parsed: ParsedActiveSyncQuery, headers: HeaderMap, body: &[u8], ) -> axum::response::Response`

# Calls

- [handle_parsed_request](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)

# Called by

- [base64_sync_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches.md)
- [base64_ping_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_ping_request_dispatches.md)
- [base64_send_mail_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches.md)
- [base64_smart_reply_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_reply_request_dispatches.md)
- [base64_smart_forward_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_smart_forward_request_dispatches.md)
- [base64_move_items_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_move_items_request_dispatches.md)
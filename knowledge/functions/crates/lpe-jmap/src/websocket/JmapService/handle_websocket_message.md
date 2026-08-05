---
type: Rust Method
title: handle_websocket_message
resource: crates/lpe-jmap/src/websocket.rs#L96-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/websocket/websocket_request_id
  - functions/crates/lpe-jmap/src/websocket/parse_websocket_object
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error_object
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/websocket/normalize_push_data_types
  - functions/crates/lpe-jmap/src/websocket/JmapService/enable_push
---

# Signature

`async fn handle_websocket_message( &self, socket: &mut WebSocket, account: &AuthenticatedAccount, subscription: &mut PushSubscription, message: Message, ) -> Result<()>`

# Calls

- [send_request_error](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [websocket_request_id](../../../../../../functions/crates/lpe-jmap/src/websocket/websocket_request_id.md)
- [parse_websocket_object](../../../../../../functions/crates/lpe-jmap/src/websocket/parse_websocket_object.md)
- [send_request_error_object](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error_object.md)
- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [normalize_push_data_types](../../../../../../functions/crates/lpe-jmap/src/websocket/normalize_push_data_types.md)
- [enable_push](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)
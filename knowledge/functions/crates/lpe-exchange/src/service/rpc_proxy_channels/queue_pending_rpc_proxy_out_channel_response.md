---
type: Rust Function
title: queue_pending_rpc_proxy_out_channel_response
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L92-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/pending_rpc_proxy_out_channel_responses
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response
---

# Signature

`pub(super) fn queue_pending_rpc_proxy_out_channel_response( query: &str, virtual_connection_cookie: [u8; 16], bytes: Vec<u8>, )`

# Calls

- [pending_rpc_proxy_out_channel_responses](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/pending_rpc_proxy_out_channel_responses.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [log_and_forward_rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response.md)
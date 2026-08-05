---
type: Rust Function
title: debug_payload_preview_hex
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L188-L194
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_limit
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
---

# Signature

`pub(crate) fn debug_payload_preview_hex(bytes: &[u8]) -> String`

# Calls

- [debug_payload_preview_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_limit.md)
- [hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
- [rpc_proxy_binary_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)
- [rpc_proxy_held_open_binary_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response.md)
- [spawn_rpc_proxy_in_data_drain](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain.md)
- [log_and_forward_rpc_proxy_in_channel_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response.md)
- [log_rpc_proxy_connection](../../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)
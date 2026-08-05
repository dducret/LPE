---
type: Rust Function
title: consume_pending_rpc_proxy_out_channel_responses
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L112-L138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/pending_rpc_proxy_out_channel_responses
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn consume_pending_rpc_proxy_out_channel_responses( query: &str, virtual_connection_cookie: Option<[u8; 16]>, ) -> Vec<u8>`

# Calls

- [pending_rpc_proxy_out_channel_responses](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/pending_rpc_proxy_out_channel_responses.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
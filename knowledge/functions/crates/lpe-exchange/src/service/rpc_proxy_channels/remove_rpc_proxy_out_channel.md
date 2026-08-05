---
type: Rust Function
title: remove_rpc_proxy_out_channel
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L162-L171
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn remove_rpc_proxy_out_channel( query: &str, virtual_connection_cookie: Option<[u8; 16]>, )`

# Calls

- [rpc_proxy_out_channels](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie.md)
- [rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel.md)
- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
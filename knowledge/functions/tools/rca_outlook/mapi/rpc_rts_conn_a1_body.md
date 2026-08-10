---
type: Python Function
title: rpc_rts_conn_a1_body
resource: tools/rca_outlook/mapi.py#L197-L209
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping
---

# Signature

`def rpc_rts_conn_a1_body(receive_window_size: int = 0x00010000) -> bytes:`

# Called by

- [check_rpc_proxy_auth](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth.md)
- [check_rpc_proxy_mailstore_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping.md)
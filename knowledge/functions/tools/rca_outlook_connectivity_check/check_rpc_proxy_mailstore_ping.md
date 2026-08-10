---
type: Python Function
title: check_rpc_proxy_mailstore_ping
resource: tools/rca_outlook_connectivity_check.py#L1486-L1545
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/mapi/rpc_rts_conn_a1_body
  - functions/tools/rca_outlook/mapi/rpc_rts_conn_b1_body
  - functions/tools/rca_outlook/http/content_type
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_rpc_proxy_mailstore_ping( base_url: str, email: str, password: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [rpc_rts_conn_a1_body](../../../functions/tools/rca_outlook/mapi/rpc_rts_conn_a1_body.md)
- [rpc_rts_conn_b1_body](../../../functions/tools/rca_outlook/mapi/rpc_rts_conn_b1_body.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)
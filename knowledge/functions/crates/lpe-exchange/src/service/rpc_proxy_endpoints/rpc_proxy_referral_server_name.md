---
type: Rust Function
title: rpc_proxy_referral_server_name
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L361-L368
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response
---

# Signature

`fn rpc_proxy_referral_server_name(endpoint_query: &str) -> String`

# Called by

- [rpc_proxy_rfri_get_new_dsa_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
---
type: Rust Function
title: rpc_proxy_referral_server_name_for_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L370-L388
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal
---

# Signature

`fn rpc_proxy_referral_server_name_for_principal( endpoint_query: &str, principal: &AccountPrincipal, ) -> String`

# Called by

- [rpc_proxy_rfri_get_new_dsa_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal.md)
- [rpc_proxy_rfri_get_fqdn_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal.md)
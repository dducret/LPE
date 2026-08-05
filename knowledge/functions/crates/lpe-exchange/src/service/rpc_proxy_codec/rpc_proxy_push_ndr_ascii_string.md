---
type: Rust Function
title: rpc_proxy_push_ndr_ascii_string
resource: crates/lpe-exchange/src/service/rpc_proxy_codec.rs#L16-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row
---

# Signature

`pub(super) fn rpc_proxy_push_ndr_ascii_string(buffer: &mut Vec<u8>, value: &str)`

# Calls

- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_rfri_get_new_dsa_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_new_dsa_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
- [rpc_proxy_rfri_get_fqdn_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal.md)
- [rpc_proxy_nspi_resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)
- [rpc_proxy_push_property_row](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row.md)
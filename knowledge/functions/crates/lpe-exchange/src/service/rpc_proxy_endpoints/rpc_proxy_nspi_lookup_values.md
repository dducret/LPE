---
type: Rust Function
title: rpc_proxy_nspi_lookup_values
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1119-L1129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_ascii_lookup_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_utf16_lookup_values
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback
---

# Signature

`pub(super) fn rpc_proxy_nspi_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [rpc_proxy_nspi_requested_smtp_address](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rpc_proxy_nspi_ascii_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_ascii_lookup_values.md)
- [rpc_proxy_nspi_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_utf16_lookup_values.md)

# Called by

- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)
- [rpc_proxy_filter_nspi_entries](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries.md)
- [rpc_proxy_requested_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry.md)
- [rpc_proxy_address_book_check_name_fallback](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback.md)
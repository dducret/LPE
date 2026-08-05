---
type: Rust Function
title: nspi_hierarchy_table_response
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L90-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_request_flags
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_response_contract
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_response
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_info_response
---

# Signature

`fn nspi_hierarchy_table_response( principal: &AccountPrincipal, request: &[u8], request_id: &str, request_type: &'static str, context_name: &'static str, ) -> Response`

# Calls

- [nspi_request_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_request_flags.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [log_nspi_response_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_response_contract.md)
- [mapi_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [nspi_special_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_response.md)
- [nspi_hierarchy_info_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_info_response.md)
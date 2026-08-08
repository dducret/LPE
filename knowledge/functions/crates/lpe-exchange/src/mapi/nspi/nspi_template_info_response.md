---
type: Rust Function
title: nspi_template_info_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1000-L1030
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) async fn nspi_template_info_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [allocate_principal_nspi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [nspi_entry_property_value_list](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
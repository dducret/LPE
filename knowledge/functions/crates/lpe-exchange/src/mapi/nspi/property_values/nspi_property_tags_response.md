---
type: Rust Function
title: nspi_property_tags_response
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L151-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn nspi_property_tags_response( request_type: &str, request_id: &str, ) -> Response`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_large_property_tag_array](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array.md)
- [mapi_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
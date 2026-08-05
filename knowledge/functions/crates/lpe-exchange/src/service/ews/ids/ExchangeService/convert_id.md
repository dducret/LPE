---
type: Rust Method
title: convert_id
resource: crates/lpe-exchange/src/service/ews/ids.rs#L262-L289
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  - functions/crates/lpe-exchange/src/service/ews/ids/requested_convert_ids
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_canonical_ews_object_id
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_id_xml
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_id_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn convert_id(&self, request: &str) -> Result<String>`

# Calls

- [attribute_value_after](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [requested_convert_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/requested_convert_ids.md)
- [canonical_ews_object_id_from_convert_source](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source.md)
- [convert_canonical_ews_object_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_canonical_ews_object_id.md)
- [convert_id_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_xml.md)
- [convert_id_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_success_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
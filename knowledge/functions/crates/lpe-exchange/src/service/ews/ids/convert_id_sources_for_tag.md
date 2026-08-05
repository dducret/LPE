---
type: Rust Function
title: convert_id_sources_for_tag
resource: crates/lpe-exchange/src/service/ews/ids.rs#L89-L119
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/requested_convert_ids
---

# Signature

`fn convert_id_sources_for_tag(request: &str, local_name: &str) -> Vec<ConvertIdSource>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [requested_convert_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/requested_convert_ids.md)
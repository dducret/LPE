---
type: Rust Function
title: requested_attachment_ids
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L32-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/get_attachment
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/delete_attachment
---

# Signature

`pub(in crate::service) fn requested_attachment_ids(request: &str) -> Vec<String>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [get_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/get_attachment.md)
- [delete_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/delete_attachment.md)
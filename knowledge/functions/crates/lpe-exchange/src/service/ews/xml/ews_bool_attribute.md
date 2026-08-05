---
type: Rust Function
title: ews_bool_attribute
resource: crates/lpe-exchange/src/service/ews/xml.rs#L172-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk
---

# Signature

`pub(in crate::service) fn ews_bool_attribute(body: &str, tag: &str, attr: &str) -> Option<bool>`

# Calls

- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)

# Called by

- [mark_as_junk](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk.md)
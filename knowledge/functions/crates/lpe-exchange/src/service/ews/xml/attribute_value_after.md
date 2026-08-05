---
type: Rust Function
title: attribute_value_after
resource: crates/lpe-exchange/src/service/ews/xml.rs#L160-L170
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references
  - functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/xml/ews_bool_attribute
  - functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute
---

# Signature

`pub(in crate::service) fn attribute_value_after<'a>( body: &'a str, tag: &str, attr: &str, ) -> Option<&'a str>`

# Calls

- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)

# Called by

- [convert_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [delete_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [requested_item_references](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references.md)
- [bounded_ews_rule_to_sieve](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve.md)
- [parse_ews_user_configuration_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [ews_bool_attribute](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_bool_attribute.md)
- [ews_usize_attribute](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute.md)
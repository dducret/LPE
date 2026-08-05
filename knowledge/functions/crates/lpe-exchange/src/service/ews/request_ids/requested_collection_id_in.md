---
type: Rust Function
title: requested_collection_id_in
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L67-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id
---

# Signature

`pub(in crate::service) fn requested_collection_id_in<'a>( request: &'a str, wrapper: &str, ) -> Option<&'a str>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [requested_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id.md)
- [requested_sync_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id.md)
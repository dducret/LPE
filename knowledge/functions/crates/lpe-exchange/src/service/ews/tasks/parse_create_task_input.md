---
type: Rust Function
title: parse_create_task_input
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L103-L138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical
  - functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_ews_task_priority
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn parse_create_task_input( principal: &AccountPrincipal, request: &str, ) -> Result<UpsertClientTaskInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [ews_task_status_to_canonical](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical.md)
- [requested_task_list_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id.md)
- [parse_ews_task_priority](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_ews_task_priority.md)
- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
---
type: Rust Function
title: parse_update_task_input
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L140-L203
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical
  - functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id
  - functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text
  - functions/crates/lpe-exchange/src/service/ews/fields/String/emptystringfallback/if_empty
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_ews_task_priority
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`pub(in crate::service) fn parse_update_task_input( principal: &AccountPrincipal, existing: &ClientTask, request: &str, ) -> Result<UpsertClientTaskInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [ews_task_status_to_canonical](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical.md)
- [requested_task_list_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id.md)
- [deleted_or_updated_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text.md)
- [if_empty](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/String/emptystringfallback/if_empty.md)
- [parse_ews_task_priority](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_ews_task_priority.md)
- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
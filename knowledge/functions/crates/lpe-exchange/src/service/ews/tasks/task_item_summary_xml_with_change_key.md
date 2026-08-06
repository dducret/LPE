---
type: Rust Function
title: task_item_summary_xml_with_change_key
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L7-L32
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
---

# Signature

`pub(in crate::service) fn task_item_summary_xml_with_change_key( task: &ClientTask, change_key: &str, ) -> String`

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
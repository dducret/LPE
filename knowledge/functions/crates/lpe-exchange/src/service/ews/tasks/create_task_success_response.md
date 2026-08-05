---
type: Rust Function
title: create_task_success_response
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L57-L89
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn create_task_success_response( task: &ClientTask, change_key: &str, ) -> String`

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
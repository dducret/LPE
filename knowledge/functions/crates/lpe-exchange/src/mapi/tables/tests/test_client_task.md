---
type: Rust Function
title: test_client_task
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L27-L51
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_due_date
---

# Signature

`fn test_client_task(title: &str, due_at: Option<&str>, updated_at: &str) -> ClientTask`

# Called by

- [task_default_view_sort_orders_by_due_date](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_due_date.md)
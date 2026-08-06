---
type: Rust Function
title: test_journal_entry
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L54-L69
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/journal_default_view_sort_orders_by_log_start
---

# Signature

`fn test_journal_entry(subject: &str, starts_at: Option<&str>, updated_at: &str) -> JournalEntry`

# Called by

- [journal_default_view_sort_orders_by_log_start](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/journal_default_view_sort_orders_by_log_start.md)
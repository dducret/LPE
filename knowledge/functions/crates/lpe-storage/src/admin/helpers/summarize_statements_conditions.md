---
type: Rust Function
title: summarize_statements_conditions
resource: crates/lpe-storage/src/admin/helpers.rs#L72-L80
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/helpers/collect_statement_conditions
  called_by:
  - functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries
---

# Signature

`fn summarize_statements_conditions(statements: &[Statement]) -> String`

# Calls

- [collect_statement_conditions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/collect_statement_conditions.md)

# Called by

- [mailbox_rule_summaries](../../../../../../functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries.md)
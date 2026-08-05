---
type: Rust Function
title: summarize_statements_actions
resource: crates/lpe-storage/src/admin/helpers.rs#L161-L169
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/helpers/collect_statement_actions
  called_by:
  - functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries
---

# Signature

`fn summarize_statements_actions(statements: &[Statement]) -> String`

# Calls

- [collect_statement_actions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/collect_statement_actions.md)

# Called by

- [mailbox_rule_summaries](../../../../../../functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries.md)
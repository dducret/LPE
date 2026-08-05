---
type: Rust Function
title: mailbox_rule_summaries
resource: crates/lpe-storage/src/admin/helpers.rs#L59-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/helpers/summarize_statements_conditions
  - functions/crates/lpe-storage/src/admin/helpers/summarize_statements_actions
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/list_mailbox_rules
---

# Signature

`pub(super) fn mailbox_rule_summaries(content: &str) -> (String, String)`

# Calls

- [summarize_statements_conditions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_conditions.md)
- [summarize_statements_actions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_actions.md)

# Called by

- [list_mailbox_rules](../../../../../../functions/crates/lpe-storage/src/admin/Storage/list_mailbox_rules.md)
---
type: Rust Function
title: collect_statement_actions
resource: crates/lpe-storage/src/admin/helpers.rs#L171-L188
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/admin/helpers/summarize_action
  called_by:
  - functions/crates/lpe-storage/src/admin/helpers/summarize_statements_actions
---

# Signature

`fn collect_statement_actions(statements: &[Statement], parts: &mut Vec<String>)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [summarize_action](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_action.md)

# Called by

- [summarize_statements_actions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_actions.md)
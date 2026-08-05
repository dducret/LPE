---
type: Rust Function
title: collect_statement_conditions
resource: crates/lpe-storage/src/admin/helpers.rs#L82-L98
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/admin/helpers/summarize_test
  called_by:
  - functions/crates/lpe-storage/src/admin/helpers/summarize_statements_conditions
---

# Signature

`fn collect_statement_conditions(statements: &[Statement], parts: &mut Vec<String>)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [summarize_test](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_test.md)

# Called by

- [summarize_statements_conditions](../../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_conditions.md)
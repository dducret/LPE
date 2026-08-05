---
type: Rust Method
title: parse_statement
resource: crates/lpe-core/src/sieve.rs#L397-L402
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  - functions/crates/lpe-core/src/sieve/Parser/parse_if
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_script
  - functions/crates/lpe-core/src/sieve/Parser/parse_block
---

# Signature

`fn parse_statement(&mut self) -> Result<Statement>`

# Calls

- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [parse_if](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_if.md)
- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)

# Called by

- [parse_script](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_script.md)
- [parse_block](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_block.md)
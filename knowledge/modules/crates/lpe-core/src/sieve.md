---
type: Rust Module
title: sieve
resource: crates/lpe-core/src/sieve.rs#L1-L754
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/std-collections-btreemap
  - external/super
  member_of:
  - packages/crates/lpe-core
---

# Contains

- [Script](../../../../classes/crates/lpe-core/src/sieve/Script.md)
- [Statement](../../../../classes/crates/lpe-core/src/sieve/Statement.md)
- [Test](../../../../classes/crates/lpe-core/src/sieve/Test.md)
- [MatchType](../../../../classes/crates/lpe-core/src/sieve/MatchType.md)
- [Action](../../../../classes/crates/lpe-core/src/sieve/Action.md)
- [MessageContext](../../../../classes/crates/lpe-core/src/sieve/MessageContext.md)
- [VacationAction](../../../../classes/crates/lpe-core/src/sieve/VacationAction.md)
- [ExecutionOutcome](../../../../classes/crates/lpe-core/src/sieve/ExecutionOutcome.md)
- [default](../../../../functions/crates/lpe-core/src/sieve/ExecutionOutcome/default/default.md)
- [Token](../../../../classes/crates/lpe-core/src/sieve/Token.md)
- [parse_script](../../../../functions/crates/lpe-core/src/sieve/parse_script.md)
- [evaluate_script](../../../../functions/crates/lpe-core/src/sieve/evaluate_script.md)
- [execute_block](../../../../functions/crates/lpe-core/src/sieve/execute_block.md)
- [execute_action](../../../../functions/crates/lpe-core/src/sieve/execute_action.md)
- [evaluate_test](../../../../functions/crates/lpe-core/src/sieve/evaluate_test.md)
- [matches_any](../../../../functions/crates/lpe-core/src/sieve/matches_any.md)
- [extract_addresses](../../../../functions/crates/lpe-core/src/sieve/extract_addresses.md)
- [tokenize](../../../../functions/crates/lpe-core/src/sieve/tokenize.md)
- [is_identifier_start](../../../../functions/crates/lpe-core/src/sieve/is_identifier_start.md)
- [is_identifier_char](../../../../functions/crates/lpe-core/src/sieve/is_identifier_char.md)
- [Parser](../../../../classes/crates/lpe-core/src/sieve/Parser.md)
- [parse_script](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_script.md)
- [parse_statement](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)
- [parse_if](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_if.md)
- [parse_block](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_block.md)
- [parse_test](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)
- [parse_test_list](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test_list.md)
- [parse_action](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)
- [parse_match_type](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_match_type.md)
- [parse_string_list](../../../../functions/crates/lpe-core/src/sieve/Parser/parse_string_list.md)
- [expect_string](../../../../functions/crates/lpe-core/src/sieve/Parser/expect_string.md)
- [expect_identifier](../../../../functions/crates/lpe-core/src/sieve/Parser/expect_identifier.md)
- [expect_number](../../../../functions/crates/lpe-core/src/sieve/Parser/expect_number.md)
- [expect_semicolon](../../../../functions/crates/lpe-core/src/sieve/Parser/expect_semicolon.md)
- [expect](../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [consume_identifier](../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [consume](../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)
- [peek](../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [is_eof](../../../../functions/crates/lpe-core/src/sieve/Parser/is_eof.md)
- [validate_requirements](../../../../functions/crates/lpe-core/src/sieve/validate_requirements.md)
- [context](../../../../functions/crates/lpe-core/src/sieve/context.md)
- [parses_requirements_and_if_blocks](../../../../functions/crates/lpe-core/src/sieve/parses_requirements_and_if_blocks.md)
- [rejects_unsupported_requirements](../../../../functions/crates/lpe-core/src/sieve/rejects_unsupported_requirements.md)
- [evaluates_fileinto_and_stop](../../../../functions/crates/lpe-core/src/sieve/evaluates_fileinto_and_stop.md)
- [evaluates_redirect_and_vacation_without_cancelling_keep](../../../../functions/crates/lpe-core/src/sieve/evaluates_redirect_and_vacation_without_cancelling_keep.md)
- [discard_cancels_keep](../../../../functions/crates/lpe-core/src/sieve/discard_cancels_keep.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `std::collections::BTreeMap`
- `super::*`

# Member of

- [lpe-core](../../../../packages/crates/lpe-core.md)
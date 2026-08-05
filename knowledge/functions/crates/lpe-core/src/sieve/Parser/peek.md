---
type: Rust Method
title: peek
resource: crates/lpe-core/src/sieve.rs#L622-L624
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-core/src/sieve/tokenize
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  - functions/crates/lpe-core/src/sieve/Parser/consume
  - functions/crates/lpe-managesieve/src/parse/parse_request_line
  - functions/crates/lpe-managesieve/src/parse/parse_atom
  - functions/crates/lpe-managesieve/src/parse/parse_literal_marker
---

# Signature

`fn peek(&self) -> Option<&'a Token>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [tokenize](../../../../../../functions/crates/lpe-core/src/sieve/tokenize.md)
- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [consume](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)
- [parse_request_line](../../../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)
- [parse_atom](../../../../../../functions/crates/lpe-managesieve/src/parse/parse_atom.md)
- [parse_literal_marker](../../../../../../functions/crates/lpe-managesieve/src/parse/parse_literal_marker.md)
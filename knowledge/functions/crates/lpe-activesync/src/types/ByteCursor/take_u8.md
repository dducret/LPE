---
type: Rust Method
title: take_u8
resource: crates/lpe-activesync/src/types.rs#L223-L225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_exact
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_base64_query
---

# Signature

`fn take_u8(&mut self) -> Result<u8>`

# Calls

- [take_exact](../../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_exact.md)

# Called by

- [parse_base64_query](../../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)
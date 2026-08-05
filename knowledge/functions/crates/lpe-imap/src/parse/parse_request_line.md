---
type: Rust Function
title: parse_request_line
resource: crates/lpe-imap/src/parse.rs#L11-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub(crate) fn parse_request_line(line: &str) -> Result<RequestLine>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
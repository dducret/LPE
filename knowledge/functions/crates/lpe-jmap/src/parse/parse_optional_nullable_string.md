---
type: Rust Function
title: parse_optional_nullable_string
resource: crates/lpe-jmap/src/parse.rs#L85-L94
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
---

# Signature

`pub(crate) fn parse_optional_nullable_string( value: Option<&Value>, ) -> Result<Option<Option<String>>>`

# Called by

- [parse_draft_mutation](../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)
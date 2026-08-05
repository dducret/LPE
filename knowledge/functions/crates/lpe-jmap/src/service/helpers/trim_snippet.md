---
type: Rust Function
title: trim_snippet
resource: crates/lpe-jmap/src/service/helpers.rs#L872-L879
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/values/search_snippet_to_value
---

# Signature

`pub(crate) fn trim_snippet(value: &str, max_chars: usize) -> String`

# Called by

- [search_snippet_to_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/search_snippet_to_value.md)
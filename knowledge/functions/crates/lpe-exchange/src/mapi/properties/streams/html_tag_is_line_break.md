---
type: Rust Function
title: html_tag_is_line_break
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L944-L954
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/plain_text_from_html_body
---

# Signature

`fn html_tag_is_line_break(tag: &str) -> bool`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [plain_text_from_html_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/plain_text_from_html_body.md)
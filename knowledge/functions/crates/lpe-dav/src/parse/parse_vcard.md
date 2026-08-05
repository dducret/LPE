---
type: Rust Function
title: parse_vcard
resource: crates/lpe-dav/src/parse.rs#L10-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/unfolded_lines
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-dav/src/parse/text_unescape
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn parse_vcard( id: Uuid, account_id: Uuid, body: &[u8], ) -> Result<UpsertClientContactInput>`

# Calls

- [unfolded_lines](../../../../../functions/crates/lpe-dav/src/parse/unfolded_lines.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [text_unescape](../../../../../functions/crates/lpe-dav/src/parse/text_unescape.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
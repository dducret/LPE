---
type: Rust Function
title: parse_vtodo
resource: crates/lpe-dav/src/parse.rs#L146-L214
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/unfolded_lines
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-dav/src/parse/text_unescape
  - functions/crates/lpe-dav/src/parse/task_status_from_vtodo_status
  - functions/crates/lpe-dav/src/parse/parse_ical_timestamp
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn parse_vtodo( id: Uuid, account_id: Uuid, collection_id: Option<&str>, body: &[u8], ) -> Result<UpsertClientTaskInput>`

# Calls

- [unfolded_lines](../../../../../functions/crates/lpe-dav/src/parse/unfolded_lines.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [text_unescape](../../../../../functions/crates/lpe-dav/src/parse/text_unescape.md)
- [task_status_from_vtodo_status](../../../../../functions/crates/lpe-dav/src/parse/task_status_from_vtodo_status.md)
- [parse_ical_timestamp](../../../../../functions/crates/lpe-dav/src/parse/parse_ical_timestamp.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
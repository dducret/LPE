---
type: Rust Function
title: parse_plain_query
resource: crates/lpe-activesync/src/types.rs#L68-L90
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/decode_query_component
  - functions/crates/lpe-activesync/src/types/save_in_sent_options
  called_by:
  - functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query
---

# Signature

`fn parse_plain_query(raw_query: &str) -> Result<ParsedActiveSyncQuery>`

# Calls

- [decode_query_component](../../../../../functions/crates/lpe-activesync/src/types/decode_query_component.md)
- [save_in_sent_options](../../../../../functions/crates/lpe-activesync/src/types/save_in_sent_options.md)

# Called by

- [from_raw_query](../../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)
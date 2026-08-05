---
type: Rust Function
title: options_handler
resource: crates/lpe-activesync/src/app.rs#L27-L37
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query
  - functions/crates/lpe-activesync/src/app/options_response_for_store
---

# Signature

`async fn options_handler( State(storage): State<Storage>, RawQuery(raw_query): RawQuery, headers: HeaderMap, ) -> Response`

# Calls

- [from_raw_query](../../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)
- [options_response_for_store](../../../../../functions/crates/lpe-activesync/src/app/options_response_for_store.md)
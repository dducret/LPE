---
type: Rust Function
title: post_handler
resource: crates/lpe-activesync/src/app.rs#L50-L68
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`async fn post_handler( State(storage): State<Storage>, RawQuery(raw_query): RawQuery, headers: HeaderMap, body: Bytes, ) -> Response`

# Calls

- [from_raw_query](../../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)
- [handle_parsed_request](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
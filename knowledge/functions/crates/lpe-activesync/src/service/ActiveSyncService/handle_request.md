---
type: Rust Method
title: handle_request
resource: crates/lpe-activesync/src/service.rs#L146-L161
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(crate) async fn handle_request( &self, query: ActiveSyncQuery, headers: &HeaderMap, body: &[u8], ) -> Result<Response>`

# Calls

- [handle_parsed_request](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
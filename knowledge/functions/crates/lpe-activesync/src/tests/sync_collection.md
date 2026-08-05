---
type: Rust Function
title: sync_collection
resource: crates/lpe-activesync/src/tests.rs#L2010-L2036
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/tests/active_sync_query
  - functions/crates/lpe-activesync/src/tests/decode_response_body
---

# Signature

`async fn sync_collection( service: &ActiveSyncService<FakeStore>, collection_id: &str, sync_key: &str, device_id: &str, ) -> WbxmlNode`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [active_sync_query](../../../../../functions/crates/lpe-activesync/src/tests/active_sync_query.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)
---
type: Rust Method
title: store_activesync_sync_state
resource: crates/lpe-activesync/src/tests.rs#L1249-L1272
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn store_activesync_sync_state<'a>( &'a self, account_id: Uuid, device_id: &'a str, collection_id: &'a str, sync_key: &'a str, snapshot_json: String, ) -> StoreFuture<'a, ()>`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
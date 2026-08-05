---
type: Rust Method
title: delete_search_folder
resource: crates/lpe-exchange/src/tests/mod.rs#L9572-L9597
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_search_folder<'a>( &'a self, _account_id: Uuid, search_folder_id: Uuid, ) -> StoreFuture<'a, ()>`

# Calls

- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
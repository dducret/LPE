---
type: Rust Method
title: delete_dav_task
resource: crates/lpe-dav/src/tests.rs#L726-L747
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`fn delete_dav_task<'a>( &'a self, _principal_account_id: Uuid, task_id: Uuid, ) -> lpe_mail_auth::StoreFuture<'a, ()>`

# Calls

- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
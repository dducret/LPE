---
type: Rust Method
title: notification_folder_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L143-L154
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request
---

# Signature

`pub(in crate::mapi) fn notification_folder_id(&self) -> Option<u64>`

# Calls

- [notification_want_whole_store](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store.md)
- [notification_types](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [notification_registration_from_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request.md)
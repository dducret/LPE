---
type: Rust Method
title: notification_types
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L123-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id
---

# Signature

`pub(in crate::mapi) fn notification_types(&self) -> Option<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [notification_registration_from_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request.md)
- [notification_want_whole_store](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store.md)
- [notification_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id.md)
---
type: Rust Method
title: notification_want_whole_store
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L131-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id
---

# Signature

`pub(in crate::mapi) fn notification_want_whole_store(&self) -> Option<bool>`

# Calls

- [notification_types](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [notification_registration_from_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request.md)
- [notification_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id.md)
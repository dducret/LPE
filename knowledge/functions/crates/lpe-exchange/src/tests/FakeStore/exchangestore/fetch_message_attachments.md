---
type: Rust Method
title: fetch_message_attachments
resource: crates/lpe-exchange/src/tests/mod.rs#L11021-L11034
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
---

# Signature

`fn fetch_message_attachments<'a>( &'a self, _account_id: Uuid, message_id: Uuid, ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_submit_attachments_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
---
type: Rust Function
title: mapi_submit_attachments_from_email
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1175-L1210
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email
---

# Signature

`pub(super) async fn mapi_submit_attachments_from_email<S>( store: &S, account_id: Uuid, email: &JmapEmail, ) -> Result<Vec<AttachmentUploadInput>> where S: ExchangeStore,`

# Calls

- [fetch_message_attachments](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments.md)
- [fetch_attachment_content](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_submit_from_existing_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email.md)
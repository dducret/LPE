---
type: Rust Method
title: copy_folder
resource: crates/lpe-exchange/src/service.rs#L567-L611
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_public_folder_tree
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_mailbox_folder_tree
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`async fn copy_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String>`

# Calls

- [requested_public_folder_ids](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [requested_public_folder_ids_in](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [copy_public_folder_tree](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_public_folder_tree.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [public_folder_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)
- [folders_operation_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response.md)
- [requested_mailbox_folder_ids_in](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in.md)
- [copy_mailbox_folder_tree](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_mailbox_folder_tree.md)
- [mailbox_folder_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [operation_error_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
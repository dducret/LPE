---
type: Rust Method
title: update_folder
resource: crates/lpe-exchange/src/service.rs#L687-L775
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/folders/validate_supplied_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id
  - functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`async fn update_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String>`

# Calls

- [element_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [attribute_values_for_tag](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [requested_public_folder_ids](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [validate_supplied_folder_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/validate_supplied_folder_change_key.md)
- [public_folder_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_change_key.md)
- [folders_operation_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response.md)
- [public_folder_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)
- [mailbox_by_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id.md)
- [ensure_custom_mailbox](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox.md)
- [mailbox_folder_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_change_key.md)
- [mailbox_folder_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [operation_error_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
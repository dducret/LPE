---
type: Rust Function
title: is_outlook_configuration_message_class
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L516-L520
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_default_sync_tags
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`pub(crate) fn is_outlook_configuration_message_class(message_class: &str) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_open_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [format_ipm_configuration_contract_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary.md)
- [format_ipm_configuration_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)
- [associated_config_default_sync_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_default_sync_tags.md)
- [sanitize_configuration_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)
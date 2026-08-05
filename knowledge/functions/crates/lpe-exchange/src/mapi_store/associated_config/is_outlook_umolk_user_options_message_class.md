---
type: Rust Function
title: is_outlook_umolk_user_options_message_class
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L529-L531
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_stale_minimal_umolk_dictionary
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_stale_outlook_umolk_user_options_placeholder
---

# Signature

`pub(crate) fn is_outlook_umolk_user_options_message_class(message_class: &str) -> bool`

# Called by

- [record_outlook_umolk_getprops_materialization](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)
- [record_outlook_umolk_named_property_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe.md)
- [is_stale_minimal_umolk_dictionary](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_stale_minimal_umolk_dictionary.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [is_stale_outlook_umolk_user_options_placeholder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_stale_outlook_umolk_user_options_placeholder.md)
---
type: Rust Function
title: release_handle_zero_rop_buffer
resource: crates/lpe-exchange/src/mapi/store_adapter/tests.rs#L122-L124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_default_conversation_action_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_default_common_views_shortcut_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_common_views_named_view_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_folder_named_view_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_inbox_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_non_virtual_quick_step_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_unbacked_contact_associated_config_identity
---

# Signature

`fn release_handle_zero_rop_buffer() -> Vec<u8>`

# Calls

- [single_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer.md)

# Called by

- [access_plan_does_not_fetch_virtual_default_conversation_action_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_default_conversation_action_identity.md)
- [access_plan_does_not_fetch_default_common_views_shortcut_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_default_common_views_shortcut_identity.md)
- [access_plan_fetches_common_views_named_view_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_common_views_named_view_identity.md)
- [access_plan_fetches_folder_named_view_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_folder_named_view_identity.md)
- [access_plan_does_not_fetch_virtual_inbox_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_inbox_associated_config_identity.md)
- [access_plan_fetches_non_virtual_quick_step_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_non_virtual_quick_step_associated_config_identity.md)
- [access_plan_fetches_unbacked_contact_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_unbacked_contact_associated_config_identity.md)
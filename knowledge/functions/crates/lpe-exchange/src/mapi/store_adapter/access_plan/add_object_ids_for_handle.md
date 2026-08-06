---
type: Rust Function
title: add_object_ids_for_handle
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L839-L984
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`fn add_object_ids_for_handle(plan: &mut MapiAccessPlan, object: &MapiObject)`

# Calls

- [push_unique](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique.md)
- [is_outlook_default_conversation_action_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [is_outlook_common_views_default_navigation_shortcut_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_common_views_default_navigation_shortcut_id.md)
- [is_outlook_inbox_default_associated_config_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_default_associated_config_id.md)

# Called by

- [extend_access_plan_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
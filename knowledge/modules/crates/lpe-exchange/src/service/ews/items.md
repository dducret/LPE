---
type: Rust Module
title: items
resource: crates/lpe-exchange/src/service/ews/items.rs#L1-L1673
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-super
  - external/super-requested-update-item-changes-update-item-change-content-validate-required-item-change-key-validate-supplied-item-change-key
  - external/crate-service-ews-request-ids-requesteditemreference
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [get_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [find_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [update_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [create_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [send_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item.md)
- [mark_all_items_as_read](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/mark_all_items_as_read.md)
- [archive_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item.md)
- [copy_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
- [delete_item](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [validate_mutating_item_change_keys](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [validate_supplied_item_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/validate_supplied_item_change_key.md)
- [UpdateItemChange](../../../../../../classes/crates/lpe-exchange/src/service/ews/items/UpdateItemChange.md)
- [requested_update_item_changes](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/requested_update_item_changes.md)
- [update_item_change_content](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/update_item_change_content.md)
- [validate_required_item_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/validate_required_item_change_key.md)
- [stale_supplied_change_key_is_rejected_before_item_mutation](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/stale_supplied_change_key_is_rejected_before_item_mutation.md)
- [missing_required_change_key_is_a_conflict](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/missing_required_change_key_is_a_conflict.md)
- [update_item_changes_keep_each_item_payload_local](../../../../../../functions/crates/lpe-exchange/src/service/ews/items/update_item_changes_keep_each_item_payload_local.md)

# Imports

- `super::super::*`
- `super::{
        requested_update_item_changes, update_item_change_content,
        validate_required_item_change_key, validate_supplied_item_change_key,
    }`
- `crate::service::ews::request_ids::RequestedItemReference`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)
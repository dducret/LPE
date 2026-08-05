---
type: Rust Function
title: mapi_recoverable_item_id
resource: crates/lpe-exchange/src/mapi_store.rs#L951-L953
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_restores_recoverable_item_through_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_copy_is_rejected_without_restore_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_purge_reports_partial_when_canonical_store_blocks
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_delete_messages_is_bounded_rejection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_root_message_mutations_are_parseable_not_supported
---

# Signature

`pub(crate) fn mapi_recoverable_item_id(id: &Uuid) -> u64`

# Calls

- [legacy_migration_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)

# Called by

- [with_recoverable_items](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items.md)
- [mapi_over_http_restores_recoverable_item_through_canonical_store](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_restores_recoverable_item_through_canonical_store.md)
- [mapi_over_http_recoverable_copy_is_rejected_without_restore_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_copy_is_rejected_without_restore_side_effect.md)
- [mapi_over_http_recoverable_purge_reports_partial_when_canonical_store_blocks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_purge_reports_partial_when_canonical_store_blocks.md)
- [mapi_over_http_recoverable_delete_messages_is_bounded_rejection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_delete_messages_is_bounded_rejection.md)
- [mapi_over_http_recoverable_root_message_mutations_are_parseable_not_supported](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_recoverable_root_message_mutations_are_parseable_not_supported.md)
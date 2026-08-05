---
type: Rust Method
title: message_for_canonical_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L754-L758
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row
---

# Signature

`pub(crate) fn message_for_canonical_id(&self, canonical_id: Uuid) -> Option<&MapiMessage>`

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [normal_message_sync_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [serialize_message_property_row_in_snapshot_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [serialize_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row.md)
- [serialize_categorized_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row.md)
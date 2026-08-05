---
type: Rust Function
title: function_body
resource: crates/lpe-storage/src/schema_contract.rs#L142-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/collaboration_mutations_write_object_level_change_rows
  - functions/crates/lpe-storage/src/schema_contract/collaboration_upserts_mark_inserted_rows_as_created
  - functions/crates/lpe-storage/src/schema_contract/collaboration_deletes_write_tombstones
  - functions/crates/lpe-storage/src/schema_contract/mailbox_count_mutations_recalculate_from_visible_memberships
  - functions/crates/lpe-storage/src/schema_contract/mailbox_moves_create_target_membership_and_tombstone_source_uid
  - functions/crates/lpe-storage/src/schema_contract/jmap_email_projection_preserves_multi_mailbox_memberships
  - functions/crates/lpe-storage/src/schema_contract/imported_and_inbound_mail_persist_message_date_metadata
  - functions/crates/lpe-storage/src/schema_contract/jmap_mailbox_storage_uses_shared_name_policy
---

# Signature

`fn function_body<'a>(source: &'a str, signature: &str) -> &'a str`

# Called by

- [collaboration_mutations_write_object_level_change_rows](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_mutations_write_object_level_change_rows.md)
- [collaboration_upserts_mark_inserted_rows_as_created](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_upserts_mark_inserted_rows_as_created.md)
- [collaboration_deletes_write_tombstones](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_deletes_write_tombstones.md)
- [mailbox_count_mutations_recalculate_from_visible_memberships](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_count_mutations_recalculate_from_visible_memberships.md)
- [mailbox_moves_create_target_membership_and_tombstone_source_uid](../../../../../functions/crates/lpe-storage/src/schema_contract/mailbox_moves_create_target_membership_and_tombstone_source_uid.md)
- [jmap_email_projection_preserves_multi_mailbox_memberships](../../../../../functions/crates/lpe-storage/src/schema_contract/jmap_email_projection_preserves_multi_mailbox_memberships.md)
- [imported_and_inbound_mail_persist_message_date_metadata](../../../../../functions/crates/lpe-storage/src/schema_contract/imported_and_inbound_mail_persist_message_date_metadata.md)
- [jmap_mailbox_storage_uses_shared_name_policy](../../../../../functions/crates/lpe-storage/src/schema_contract/jmap_mailbox_storage_uses_shared_name_policy.md)
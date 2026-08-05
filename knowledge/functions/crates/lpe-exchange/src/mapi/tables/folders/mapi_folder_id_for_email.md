---
type: Rust Function
title: mapi_folder_id_for_email
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L929-L933
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id_for_role
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot
---

# Signature

`pub(in crate::mapi) fn mapi_folder_id_for_email(email: &JmapEmail) -> u64`

# Calls

- [try_mapi_folder_id_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id_for_role.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [restriction_matches_conversation_member_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot.md)
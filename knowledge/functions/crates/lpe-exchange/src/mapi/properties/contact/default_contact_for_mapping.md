---
type: Rust Function
title: default_contact_for_mapping
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L340-L359
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_restriction_uses_projected_folder_context
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_email_named_property_restriction_matches_primary_email
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_secondary_email_named_property_uses_emails_json
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_view_provider_array_restriction_matches_contact_email
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_view_email_alias_restriction_matches_primary_email
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_property_projects_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_search_source_columns_project_empty_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
---

# Signature

`pub(in crate::mapi) fn default_contact_for_mapping( account_id: Uuid, collection_id: &str, ) -> AccessibleContact`

# Calls

- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)

# Called by

- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)
- [contact_restriction_uses_projected_folder_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_restriction_uses_projected_folder_context.md)
- [contact_email_named_property_restriction_matches_primary_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_email_named_property_restriction_matches_primary_email.md)
- [contact_secondary_email_named_property_uses_emails_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_secondary_email_named_property_uses_emails_json.md)
- [outlook_contact_view_provider_array_restriction_matches_contact_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_view_provider_array_restriction_matches_contact_email.md)
- [outlook_contact_view_email_alias_restriction_matches_primary_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_view_email_alias_restriction_matches_primary_email.md)
- [contact_property_projects_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_property_projects_outlook_table_identity_columns.md)
- [contact_entry_id_is_private_message_entry_id_not_a_sync_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key.md)
- [outlook_contact_search_source_columns_project_empty_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contact_search_source_columns_project_empty_values.md)
- [microsoft_oxprops_message_size_projects_integer32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property.md)
- [serialize_pending_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)
---
type: Rust Function
title: normalize_table_property_tags_for_session
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L288-L295
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_alias_without_cached_mapping
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases
---

# Signature

`pub(super) fn normalize_table_property_tags_for_session( session: &MapiSession, tags: Vec<u32>, ) -> Vec<u32>`

# Calls

- [normalize_table_property_tag_for_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session.md)

# Called by

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [table_columns_normalize_stale_sharing_named_property_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias.md)
- [table_columns_normalize_stale_sharing_alias_without_cached_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_alias_without_cached_mapping.md)
- [table_columns_normalize_well_known_contact_email_named_property_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias.md)
- [table_columns_normalize_outlook_contact_view_email_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias.md)
- [table_columns_normalize_outlook_visible_inbox_view_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property.md)
- [table_columns_normalize_outlook_calendar_common_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases.md)
---
type: Rust Function
title: default_wlink_group_uuid
resource: crates/lpe-exchange/src/mapi/properties.rs#L1036-L1038
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_group_name
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_section_one_projects_favorites_group_name
  - functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_common_views_shortcuts
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_sort_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/normalize_navigation_shortcut_group_name
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
---

# Signature

`pub(crate) fn default_wlink_group_uuid() -> Uuid`

# Calls

- [default_wlink_group_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_guid.md)

# Called by

- [wlink_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_group_name.md)
- [navigation_shortcut_section_one_projects_favorites_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_section_one_projects_favorites_group_name.md)
- [persisted_common_views_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_common_views_shortcuts.md)
- [common_views_sort_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_sort_snapshot.md)
- [normalize_navigation_shortcut_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi_store/normalize_navigation_shortcut_group_name.md)
- [common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links.md)
- [mapi_navigation_shortcut_upsert_preserves_distinct_message_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_upsert_preserves_distinct_message_rows.md)
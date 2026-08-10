---
type: Rust Method
title: navigation_shortcut_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1282-L1288
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
---

# Signature

`pub(crate) fn navigation_shortcut_messages(&self) -> Vec<MapiNavigationShortcutMessage>`

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [with_navigation_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts.md)
- [canonical_common_views_fai_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages.md)
- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties.md)
- [common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)
- [mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
---
type: Rust Module
title: tables
resource: crates/lpe-exchange/src/mapi/tables.rs#L1-L828
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-permissions
  - external/super-properties
  - external/super-rop
  - external/super-session
  - external/super-sync
  - external/super-wire-mapipropertytype
  - external/super
  - external/crate-mapi-identity-conversation-members-contents-table-id-quick-step-settings-folder-id-recoverable-items-deletions-folder-id-recoverable-items-purges-folder-id-recoverable-items-root-folder-id-recoverable-items-versions-folder-id
  - external/crate-mapi-store-mapiassociatedconfigmessage-mapicommonviewnamedviewmessage-mapicommonviewsmessage-mapicontact-mapiconversationactionmessage-mapidelegatefreebusymessage-mapimessage-mapinavigationshortcutmessage-mapitask
  - external/lpe-storage-searchfolderdefinition
  - external/pub-in-crate-mapi-use-associated-contents
  - external/pub-super-use-attachments
  - external/pub-in-crate-mapi-use-calendar
  - external/pub-in-crate-mapi-use-collaboration-items
  - external/pub-in-crate-mapi-use-collapse
  - external/pub-super-use-columns
  - external/pub-in-crate-mapi-use-contents
  - external/pub-in-crate-mapi-use-controls
  - external/pub-in-crate-mapi-use-counts
  - external/deleted-items
  - external/pub-in-crate-mapi-use-diagnostics-outlook-bootstrap-row-invariant-summaries
  - external/diagnostics
  - external/pub-in-crate-mapi-use-filters-is-unrestricted-common-views-navigation-projection
  - external/filters
  - external/find
  - external/pub-in-crate-mapi-use-flags
  - external/pub-in-crate-mapi-use-folders
  - external/pub-in-crate-mapi-use-hierarchy-special-folder-property-value-with-change-number
  - external/hierarchy
  - external/pub-in-crate-mapi-use-hierarchy-hierarchy-depth-folder-ids-excluding-deleted-hierarchy-table-row-modified-mailbox-shadowed-by-active-outlook-special-folder-special-folder-property-value
  - external/pub-in-crate-mapi-use-pending
  - external/pub-in-crate-mapi-use-public-folders
  - external/query
  - external/pub-in-crate-mapi-use-query-rows
  - external/pub-in-crate-mapi-use-recipients
  - external/pub-in-crate-mapi-use-recoverable-items
  - external/pub-super-use-row-codecs
  - external/pub-in-crate-mapi-use-row-keys
  - external/pub-super-use-rules
  - external/search-folders
  - external/pub-in-crate-mapi-use-sorting
  - external/pub-in-crate-mapi-use-state
  - external/pub-in-crate-mapi-use-time
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_find_row_response](../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)

# Imports

- `super::permissions::*`
- `super::properties::*`
- `super::rop::*`
- `super::session::*`
- `super::sync::*`
- `super::wire::MapiPropertyType`
- `super::*`
- `crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
}`
- `crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiCommonViewNamedViewMessage, MapiCommonViewsMessage,
    MapiContact, MapiConversationActionMessage, MapiDelegateFreeBusyMessage, MapiMessage,
    MapiNavigationShortcutMessage, MapiTask,
}`
- `lpe_storage::SearchFolderDefinition`
- `pub(in crate::mapi) use associated_contents::*`
- `pub(super) use attachments::*`
- `pub(in crate::mapi) use calendar::*`
- `pub(in crate::mapi) use collaboration_items::*`
- `pub(in crate::mapi) use collapse::*`
- `pub(super) use columns::*`
- `pub(in crate::mapi) use contents::*`
- `pub(in crate::mapi) use controls::*`
- `pub(in crate::mapi) use counts::*`
- `deleted_items::*`
- `pub(in crate::mapi) use diagnostics::outlook_bootstrap_row_invariant_summaries`
- `diagnostics::*`
- `pub(in crate::mapi) use filters::is_unrestricted_common_views_navigation_projection`
- `filters::*`
- `find::*`
- `pub(in crate::mapi) use flags::*`
- `pub(in crate::mapi) use folders::*`
- `pub(in crate::mapi) use hierarchy::special_folder_property_value_with_change_number`
- `hierarchy::*`
- `pub(in crate::mapi) use hierarchy::{
    hierarchy_depth_folder_ids_excluding_deleted, hierarchy_table_row_modified,
    mailbox_shadowed_by_active_outlook_special_folder, special_folder_property_value,
}`
- `pub(in crate::mapi) use pending::*`
- `pub(in crate::mapi) use public_folders::*`
- `query::*`
- `pub(in crate::mapi) use query_rows::*`
- `pub(in crate::mapi) use recipients::*`
- `pub(in crate::mapi) use recoverable_items::*`
- `pub(super) use row_codecs::*`
- `pub(in crate::mapi) use row_keys::*`
- `pub(super) use rules::*`
- `search_folders::*`
- `pub(in crate::mapi) use sorting::*`
- `pub(in crate::mapi) use state::*`
- `pub(in crate::mapi) use time::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
---
type: Rust Function
title: restriction_matches_navigation_shortcut
resource: crates/lpe-exchange/src/mapi/properties.rs#L378-L386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
---

# Signature

`pub(in crate::mapi) fn restriction_matches_navigation_shortcut( restriction: Option<&MapiRestriction>, message: &MapiNavigationShortcutMessage, account_id: Uuid, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [navigation_shortcut_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)

# Called by

- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [restriction_matches_common_views_message](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
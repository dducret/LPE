---
type: Rust Function
title: default_view_uses_common_views
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L108-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported
---

# Signature

`pub(in crate::mapi) fn default_view_uses_common_views( container_class: &str, folder_id: u64, ) -> bool`

# Calls

- [default_common_views_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)

# Called by

- [folder_local_default_named_view_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported.md)
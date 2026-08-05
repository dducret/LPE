---
type: Rust Function
title: optional_folder_profile_read
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1189-L1196
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
---

# Signature

`pub(super) async fn optional_folder_profile_read<T>( read: impl std::future::Future<Output = anyhow::Result<T>>, ) -> Option<T>`

# Called by

- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
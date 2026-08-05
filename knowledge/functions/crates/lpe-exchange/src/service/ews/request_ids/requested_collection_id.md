---
type: Rust Function
title: requested_collection_id
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L63-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/refresh_sharing_folder
  - functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id
---

# Signature

`pub(in crate::service) fn requested_collection_id(request: &str) -> Option<&str>`

# Calls

- [requested_collection_id_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in.md)

# Called by

- [requested_folder_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)
- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [refresh_sharing_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/refresh_sharing_folder.md)
- [requested_task_list_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/requested_task_list_id.md)
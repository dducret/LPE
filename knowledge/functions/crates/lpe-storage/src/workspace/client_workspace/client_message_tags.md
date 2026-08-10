---
type: Rust Function
title: client_message_tags
resource: crates/lpe-storage/src/workspace/client_workspace.rs#L250-L258
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace
---

# Signature

`fn client_message_tags(role: &str, delivery_status: &str) -> Vec<String>`

# Called by

- [fetch_client_workspace](../../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace.md)
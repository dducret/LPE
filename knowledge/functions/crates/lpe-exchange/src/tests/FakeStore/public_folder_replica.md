---
type: Rust Method
title: public_folder_replica
resource: crates/lpe-exchange/src/tests/mod.rs#L4547-L4557
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_get_owning_servers_uses_ordered_canonical_replicas
---

# Signature

`fn public_folder_replica(id: &str, folder_id: &str, server_name: &str) -> PublicFolderReplica`

# Called by

- [mapi_over_http_public_folder_get_owning_servers_uses_ordered_canonical_replicas](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_get_owning_servers_uses_ordered_canonical_replicas.md)
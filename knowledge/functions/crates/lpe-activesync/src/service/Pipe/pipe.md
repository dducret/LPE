---
type: Rust Method
title: pipe
resource: crates/lpe-activesync/src/service.rs#L23-L25
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
---

# Signature

`fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T`

# Called by

- [fetch_collection_nodes](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)
---
type: Rust Function
title: parse_range
resource: crates/lpe-activesync/src/service/search.rs#L131-L144
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
---

# Signature

`fn parse_range(value: Option<&str>) -> Result<(u64, u64)>`

# Called by

- [handle_search](../../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)
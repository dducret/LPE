---
type: Rust Method
title: fetch_mapi_event_versions
resource: crates/lpe-storage/src/mapi_events.rs#L470-L531
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_mapi_event_versions( &self, principal_account_id: Uuid, event_ids: &[Uuid], ) -> Result<Vec<MapiEventVersion>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
---
type: Rust Method
title: canonical_objects
resource: crates/lpe-jmap/src/service/canonical.rs#L521-L605
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_query_ids
---

# Signature

`pub(super) async fn canonical_objects( &self, account: &AuthenticatedAccount, account_id: Uuid, data_type: &str, ) -> Result<Vec<Value>>`

# Calls

- [object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)

# Called by

- [handle_share_set](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)
- [handle_canonical_get](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get.md)
- [handle_canonical_query](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query.md)
- [handle_canonical_changes](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [canonical_query_ids](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_query_ids.md)
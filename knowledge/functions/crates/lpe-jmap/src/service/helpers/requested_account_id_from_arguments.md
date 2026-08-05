---
type: Rust Function
title: requested_account_id_from_arguments
resource: crates/lpe-jmap/src/service/helpers.rs#L7-L12
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
---

# Signature

`pub(super) fn requested_account_id_from_arguments( arguments: &Value, account: &AuthenticatedAccount, ) -> Result<Uuid>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_reminder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)
- [handle_share_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)
- [handle_search_folder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [handle_canonical_get](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get.md)
- [handle_canonical_query](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query.md)
- [handle_canonical_query_changes](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [handle_canonical_changes](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes.md)
- [handle_canonical_unsupported_write](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)
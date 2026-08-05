---
type: Python Function
title: main
resource: tools/jmap_live_shared_delegated_check.py#L416-L446
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/jmap_live_shared_delegated_check/enable_push_snapshot
  - functions/tools/jmap_live_shared_delegated_check/upsert_grants
  - functions/tools/jmap_live_shared_delegated_check/replay_push
  - functions/tools/jmap_live_shared_delegated_check/assert_push_replay
  - functions/tools/jmap_live_shared_delegated_check/assert_grantee_jmap_visibility
  - functions/tools/jmap_live_shared_delegated_check/cleanup_grants
---

# Signature

`def main() -> int:`

# Calls

- [enable_push_snapshot](../../../functions/tools/jmap_live_shared_delegated_check/enable_push_snapshot.md)
- [upsert_grants](../../../functions/tools/jmap_live_shared_delegated_check/upsert_grants.md)
- [replay_push](../../../functions/tools/jmap_live_shared_delegated_check/replay_push.md)
- [assert_push_replay](../../../functions/tools/jmap_live_shared_delegated_check/assert_push_replay.md)
- [assert_grantee_jmap_visibility](../../../functions/tools/jmap_live_shared_delegated_check/assert_grantee_jmap_visibility.md)
- [cleanup_grants](../../../functions/tools/jmap_live_shared_delegated_check/cleanup_grants.md)
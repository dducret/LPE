---
type: Rust Module
title: sync_conflicts
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L1-L197
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-btreemap
  - external/anyhow-result-anyhow-bail
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [SyncImportVersionRelation](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/SyncImportVersionRelation.md)
- [sync_import_version_relation](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation.md)
- [merge_sync_predecessor_change_lists](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/merge_sync_predecessor_change_lists.md)
- [imported_version_wins_last_writer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/imported_version_wins_last_writer.md)
- [predecessor_map_includes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/predecessor_map_includes.md)
- [parse_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/parse_predecessor_change_list.md)
- [split_xid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/split_xid.md)
- [serialize_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/serialize_predecessor_change_list.md)
- [xid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/xid.md)
- [pcl](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/pcl.md)
- [microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations.md)
- [microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors.md)
- [microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins.md)

# Imports

- `std::collections::BTreeMap`
- `anyhow::{Result, anyhow, bail}`
- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)
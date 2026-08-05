---
type: Rust Function
title: xid
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L144-L148
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins
---

# Signature

`fn xid(replica_byte: u8, counter: u64) -> Vec<u8>`

# Called by

- [microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations.md)
- [microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors.md)
- [microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins.md)
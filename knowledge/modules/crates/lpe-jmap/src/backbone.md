---
type: Rust Module
title: backbone
resource: crates/lpe-jmap/src/backbone.rs#L1-L164
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-storage-jmapemail-jmapemailaddress-jmapmailbox
  - external/serde-serialize
  - external/std-collections-hashmap
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [JmapMailboxObject](../../../../classes/crates/lpe-jmap/src/backbone/JmapMailboxObject.md)
- [JmapMailboxRights](../../../../classes/crates/lpe-jmap/src/backbone/JmapMailboxRights.md)
- [JmapEmailObject](../../../../classes/crates/lpe-jmap/src/backbone/JmapEmailObject.md)
- [JmapThreadObject](../../../../classes/crates/lpe-jmap/src/backbone/JmapThreadObject.md)
- [JmapAddressObject](../../../../classes/crates/lpe-jmap/src/backbone/JmapAddressObject.md)
- [from_canonical](../../../../functions/crates/lpe-jmap/src/backbone/JmapMailboxObject/from_canonical.md)
- [from_canonical](../../../../functions/crates/lpe-jmap/src/backbone/JmapEmailObject/from_canonical.md)
- [from_email_ids](../../../../functions/crates/lpe-jmap/src/backbone/JmapThreadObject/from_email_ids.md)
- [jmap_addresses](../../../../functions/crates/lpe-jmap/src/backbone/jmap_addresses.md)

# Imports

- `lpe_storage::{JmapEmail, JmapEmailAddress, JmapMailbox}`
- `serde::Serialize`
- `std::collections::HashMap`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)
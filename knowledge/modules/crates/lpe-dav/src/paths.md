---
type: Rust Module
title: paths
resource: crates/lpe-dav/src/paths.rs#L1-L139
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-serialize-serialize-ical-serialize-vcard-serialize-vtodo
  - external/lpe-storage-accessiblecontact-accessibleevent-davtask
  - external/std-collections-hash-map-defaulthasher-hash-hash-hasher
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [normalized_path](../../../../functions/crates/lpe-dav/src/paths/normalized_path.md)
- [contact_collection_href](../../../../functions/crates/lpe-dav/src/paths/contact_collection_href.md)
- [event_collection_href](../../../../functions/crates/lpe-dav/src/paths/event_collection_href.md)
- [dav_task_collection_id](../../../../functions/crates/lpe-dav/src/paths/dav_task_collection_id.md)
- [task_collection_id_from_path](../../../../functions/crates/lpe-dav/src/paths/task_collection_id_from_path.md)
- [task_collection_href](../../../../functions/crates/lpe-dav/src/paths/task_collection_href.md)
- [contact_href](../../../../functions/crates/lpe-dav/src/paths/contact_href.md)
- [event_href](../../../../functions/crates/lpe-dav/src/paths/event_href.md)
- [task_href](../../../../functions/crates/lpe-dav/src/paths/task_href.md)
- [collection_id_from_contact_path](../../../../functions/crates/lpe-dav/src/paths/collection_id_from_contact_path.md)
- [collection_id_from_event_path](../../../../functions/crates/lpe-dav/src/paths/collection_id_from_event_path.md)
- [collection_id_from_path](../../../../functions/crates/lpe-dav/src/paths/collection_id_from_path.md)
- [resource_id_for_contact_path](../../../../functions/crates/lpe-dav/src/paths/resource_id_for_contact_path.md)
- [resource_id_for_event_path](../../../../functions/crates/lpe-dav/src/paths/resource_id_for_event_path.md)
- [resource_id_for_task_path](../../../../functions/crates/lpe-dav/src/paths/resource_id_for_task_path.md)
- [resource_id_for_path](../../../../functions/crates/lpe-dav/src/paths/resource_id_for_path.md)
- [parse_uuid_path_segment](../../../../functions/crates/lpe-dav/src/paths/parse_uuid_path_segment.md)
- [etag](../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [etag_for_contact](../../../../functions/crates/lpe-dav/src/paths/etag_for_contact.md)
- [etag_for_event](../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)
- [etag_for_task](../../../../functions/crates/lpe-dav/src/paths/etag_for_task.md)

# Imports

- `crate::serialize::{serialize_ical, serialize_vcard, serialize_vtodo}`
- `lpe_storage::{AccessibleContact, AccessibleEvent, DavTask}`
- `std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
}`
- `uuid::Uuid`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)
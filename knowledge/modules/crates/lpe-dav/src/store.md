---
type: Rust Module
title: store
resource: crates/lpe-dav/src/store.rs#L1-L260
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-accessiblecontact-accessibleevent-collaborationcollection-davtask-storage-upsertclientcontactinput-upsertclienteventinput-upsertclienttaskinput
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [DavStore](../../../../interfaces/crates/lpe-dav/src/store/DavStore.md)
- [fetch_accessible_contact_collections](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_contact_collections.md)
- [fetch_accessible_calendar_collections](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_calendar_collections.md)
- [fetch_accessible_task_collections](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_task_collections.md)
- [fetch_accessible_contacts](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_contacts.md)
- [fetch_accessible_contacts_in_collection](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_contacts_in_collection.md)
- [fetch_accessible_events](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_events.md)
- [fetch_accessible_events_in_collection](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_accessible_events_in_collection.md)
- [fetch_dav_tasks](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_dav_tasks.md)
- [fetch_dav_tasks_by_ids](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/fetch_dav_tasks_by_ids.md)
- [create_accessible_contact](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/create_accessible_contact.md)
- [create_accessible_event](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/create_accessible_event.md)
- [update_accessible_contact](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/update_accessible_contact.md)
- [update_accessible_event](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/update_accessible_event.md)
- [upsert_dav_task](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/upsert_dav_task.md)
- [delete_accessible_contact](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/delete_accessible_contact.md)
- [delete_accessible_event](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/delete_accessible_event.md)
- [delete_dav_task](../../../../functions/crates/lpe-dav/src/store/Storage/davstore/delete_dav_task.md)

# Imports

- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, DavTask, Storage,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
}`
- `uuid::Uuid`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)
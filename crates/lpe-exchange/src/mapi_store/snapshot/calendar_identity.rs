use super::*;

// [MS-OXCFXICS] section 3.1.5.3 requires server-compatible internal IDs for
// every communication. Section 3.2.5.5 requires a persisted SourceKey to be
// emitted when present. Keep both values bound to the requesting principal's
// durable identity rows for the complete lifetime of this snapshot.
struct ScopedCalendarIdentities {
    folders: HashMap<Uuid, u64>,
    events: HashMap<Uuid, (u64, Vec<u8>)>,
}

impl ScopedCalendarIdentities {
    fn from_records(records: &[MapiIdentityRecord]) -> Self {
        let mut folders = HashMap::new();
        let mut events = HashMap::new();
        for record in records {
            match record.object_kind {
                MapiIdentityObjectKind::Mailbox => {
                    folders.insert(record.canonical_id, record.object_id);
                }
                MapiIdentityObjectKind::CalendarEvent => {
                    events.insert(
                        record.canonical_id,
                        (record.object_id, record.source_key.clone()),
                    );
                }
                _ => {}
            }
        }
        Self { folders, events }
    }

    fn folder_id(&self, collection: &CollaborationCollection) -> Result<u64> {
        if matches!(collection.id.as_str(), "default" | "calendar") {
            return Ok(crate::mapi::identity::CALENDAR_FOLDER_ID);
        }
        let canonical_id = collaboration_folder_identity_canonical_id_for_collection(
            MapiCollaborationFolderKind::Calendar,
            &collection.id,
        )
        .expect("custom Calendar collection identity");
        self.folders.get(&canonical_id).copied().ok_or_else(|| {
            anyhow!(
                "principal-scoped MAPI Calendar folder identity is missing for collection {}",
                collection.id
            )
        })
    }

    fn event(&self, canonical_id: Uuid) -> Result<(u64, Vec<u8>)> {
        self.events.get(&canonical_id).cloned().ok_or_else(|| {
            anyhow!(
                "principal-scoped MAPI Calendar Event identity is missing for canonical Event {}",
                canonical_id
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calendar_collection(id: &str) -> CollaborationCollection {
        CollaborationCollection {
            id: id.to_string(),
            kind: "calendar".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: lpe_storage::CollaborationRights::default(),
        }
    }

    #[test]
    fn default_calendar_uses_reserved_fid_without_an_identity_record() {
        let identities = ScopedCalendarIdentities::from_records(&[]);

        for collection_id in ["default", "calendar"] {
            assert_eq!(
                identities
                    .folder_id(&calendar_collection(collection_id))
                    .unwrap(),
                crate::mapi::identity::CALENDAR_FOLDER_ID
            );
        }
    }

    #[test]
    fn custom_calendar_fails_closed_without_a_principal_scoped_identity() {
        let identities = ScopedCalendarIdentities::from_records(&[]);

        assert!(identities
            .folder_id(&calendar_collection("82828282-8282-4282-9282-828282828282"))
            .is_err());
    }
}

impl MapiMailStoreSnapshot {
    pub(crate) fn empty() -> Self {
        Self::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    pub(crate) fn new(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        task_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        tasks: Vec<ClientTask>,
        folder_permissions: Vec<MapiFolderPermission>,
    ) -> Self {
        Self::build(
            mailboxes,
            emails,
            attachments,
            contact_collections,
            calendar_collections,
            task_collections,
            contacts,
            events,
            tasks,
            folder_permissions,
            None,
        )
        .expect("unscoped MAPI snapshot construction")
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_scoped_calendar_identities(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        task_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        tasks: Vec<ClientTask>,
        folder_permissions: Vec<MapiFolderPermission>,
        identity_records: &[MapiIdentityRecord],
    ) -> Result<Self> {
        let calendar_identities = ScopedCalendarIdentities::from_records(identity_records);
        Self::build(
            mailboxes,
            emails,
            attachments,
            contact_collections,
            calendar_collections,
            task_collections,
            contacts,
            events,
            tasks,
            folder_permissions,
            Some(&calendar_identities),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        task_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        tasks: Vec<ClientTask>,
        folder_permissions: Vec<MapiFolderPermission>,
        calendar_identities: Option<&ScopedCalendarIdentities>,
    ) -> Result<Self> {
        let folders = mailboxes
            .into_iter()
            .map(|mailbox| MapiFolder {
                id: mapi_folder_id(&mailbox),
                canonical_id: mailbox.id,
                mailbox,
            })
            .collect::<Vec<_>>();
        let messages = emails
            .into_iter()
            .map(|email| {
                let folder_id = mapi_message_folder_id(&email, &folders);
                let message_attachments = attachments
                    .iter()
                    .find(|(message_id, _)| *message_id == email.id)
                    .map(|(_, attachments)| attachments.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(index, attachment)| MapiAttachment {
                        attach_num: index as u32,
                        canonical_id: attachment.id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        disposition: attachment.disposition.clone(),
                        content_id: attachment.content_id.clone(),
                        size_octets: attachment.size_octets,
                    })
                    .collect::<Vec<_>>();
                MapiMessage {
                    id: mapi_message_id(&email),
                    folder_id,
                    canonical_id: email.id,
                    email,
                    attachments: message_attachments,
                }
            })
            .collect();
        let mut collaboration_folders = Vec::new();
        collaboration_folders.extend(contact_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Contacts, &collection);
            let item_count = contacts
                .iter()
                .filter(|contact| contact.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Contacts,
                collection,
                item_count,
            }
        }));
        for collection in calendar_collections {
            let id = match calendar_identities {
                Some(identities) => identities.folder_id(&collection)?,
                None => {
                    mapi_collaboration_folder_id(MapiCollaborationFolderKind::Calendar, &collection)
                }
            };
            let item_count = events
                .iter()
                .filter(|event| event.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            collaboration_folders.push(MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Calendar,
                collection,
                item_count,
            });
        }
        collaboration_folders.extend(task_collections.into_iter().map(|collection| {
            let id = mapi_collaboration_folder_id(MapiCollaborationFolderKind::Task, &collection);
            let item_count = tasks
                .iter()
                .filter(|task| task_collection_matches(task, &collection.id))
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Task,
                collection,
                item_count,
            }
        }));
        let contacts = contacts
            .into_iter()
            .filter_map(|contact| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Contacts
                            && folder.collection.id == contact.collection_id
                    })
                    .map(|folder| folder.id)?;
                Some(MapiContact {
                    id: mapi_item_id(&contact.id),
                    folder_id,
                    canonical_id: contact.id,
                    contact,
                })
            })
            .collect();
        let events = events
            .into_iter()
            .filter_map(|event| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Calendar
                            && folder.collection.id == event.collection_id
                    })
                    .map(|folder| folder.id)
                    .or_else(|| {
                        matches!(event.collection_id.as_str(), "default" | "calendar")
                            .then_some(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    })?;
                let identity = match calendar_identities {
                    Some(identities) => identities.event(event.id),
                    None => {
                        let id = mapi_item_id(&event.id);
                        Ok((id, mapi_mailstore::source_key_for_store_id(id)))
                    }
                };
                Some(identity.map(|(id, source_key)| {
                    let version = fallback_event_version(&event, id);
                    MapiEvent {
                        id,
                        source_key,
                        folder_id,
                        canonical_id: event.id,
                        event,
                        version,
                        attachments: Vec::new(),
                    }
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        let tasks = tasks
            .into_iter()
            .filter_map(|task| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Task
                            && task_collection_matches(&task, &folder.collection.id)
                    })
                    .map(|folder| folder.id)?;
                Some(MapiTask {
                    id: mapi_item_id(&task.id),
                    folder_id,
                    canonical_id: task.id,
                    task,
                })
            })
            .collect();
        Ok(Self {
            folders,
            public_folders: Vec::new(),
            public_folder_items: Vec::new(),
            public_folder_replicas: Vec::new(),
            collaboration_folders,
            messages,
            contacts,
            events,
            tasks,
            notes: Vec::new(),
            journal_entries: Vec::new(),
            search_folder_definitions: Vec::new(),
            rules: Vec::new(),
            navigation_shortcuts: Vec::new(),
            associated_configs: Vec::new(),
            associated_config_identity_ids: Vec::new(),
            conversation_actions: Vec::new(),
            delegate_freebusy_messages: Vec::new(),
            recoverable_items: Vec::new(),
            reminders: Vec::new(),
            event_reminder_overrides: HashMap::new(),
            folder_permissions,
            public_folder_permissions: Vec::new(),
            content_windows: Vec::new(),
        })
    }
}

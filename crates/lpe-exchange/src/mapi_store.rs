use lpe_mail_auth::StoreFuture;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, CollaborationCollection, JmapEmail,
    JmapMailbox,
};
use uuid::Uuid;

use crate::mapi::permissions::{
    access_from_rights, reserved_permission_rows, MapiFolderAccess, MapiFolderPermission,
};
use crate::store::ExchangeStore;
use crate::store::{MapiIdentityObjectKind, MapiIdentityRequest};

#[derive(Debug, Clone)]
pub(crate) struct MapiMailStoreSnapshot {
    folders: Vec<MapiFolder>,
    collaboration_folders: Vec<MapiCollaborationFolder>,
    messages: Vec<MapiMessage>,
    contacts: Vec<MapiContact>,
    events: Vec<MapiEvent>,
    folder_permissions: Vec<MapiFolderPermission>,
    content_windows: Vec<MapiContentTableWindow>,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiFolder {
    pub(crate) id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) mailbox: JmapMailbox,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiCollaborationFolder {
    pub(crate) id: u64,
    pub(crate) kind: MapiCollaborationFolderKind,
    pub(crate) collection: CollaborationCollection,
    pub(crate) item_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiCollaborationFolderKind {
    Contacts,
    Calendar,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) email: JmapEmail,
    pub(crate) attachments: Vec<MapiAttachment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiContact {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) contact: AccessibleContact,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiEvent {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) event: AccessibleEvent,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiAttachment {
    pub(crate) attach_num: u32,
    pub(crate) canonical_id: Uuid,
    pub(crate) file_reference: String,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) size_octets: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiContentTableWindow {
    pub(crate) folder_id: u64,
    pub(crate) view_signature: u64,
    pub(crate) offset: usize,
    pub(crate) total: usize,
    pub(crate) message_ids: Vec<Uuid>,
}

impl MapiMailStoreSnapshot {
    pub(crate) fn new(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        folder_permissions: Vec<MapiFolderPermission>,
    ) -> Self {
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
        collaboration_folders.extend(calendar_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Calendar, &collection);
            let item_count = events
                .iter()
                .filter(|event| event.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Calendar,
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
                    .map(|folder| folder.id)?;
                Some(MapiEvent {
                    id: mapi_item_id(&event.id),
                    folder_id,
                    canonical_id: event.id,
                    event,
                })
            })
            .collect();
        Self {
            folders,
            collaboration_folders,
            messages,
            contacts,
            events,
            folder_permissions,
            content_windows: Vec::new(),
        }
    }

    pub(crate) fn with_content_windows(
        mut self,
        content_windows: Vec<MapiContentTableWindow>,
    ) -> Self {
        self.content_windows = content_windows;
        self
    }

    pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox> {
        self.folders
            .iter()
            .map(|folder| folder.mailbox.clone())
            .collect()
    }

    pub(crate) fn emails(&self) -> Vec<JmapEmail> {
        self.messages
            .iter()
            .map(|message| message.email.clone())
            .collect()
    }

    pub(crate) fn content_table_window_emails(
        &self,
        folder_id: u64,
        view_signature: u64,
        offset: usize,
        limit: usize,
    ) -> Option<(usize, Vec<&JmapEmail>)> {
        let window = self.content_windows.iter().find(|window| {
            window.folder_id == folder_id
                && window.view_signature == view_signature
                && window.offset == offset
                && (window.message_ids.len() >= limit
                    || offset + window.message_ids.len() >= window.total)
        })?;
        let emails = window
            .message_ids
            .iter()
            .filter_map(|id| {
                self.messages
                    .iter()
                    .find(|message| message.canonical_id == *id)
                    .map(|message| &message.email)
            })
            .collect::<Vec<_>>();
        Some((window.total, emails))
    }

    pub(crate) fn content_table_total(&self, folder_id: u64, view_signature: u64) -> Option<usize> {
        self.content_windows
            .iter()
            .find(|window| window.folder_id == folder_id && window.view_signature == view_signature)
            .map(|window| window.total)
    }

    pub(crate) fn attachments_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
    ) -> Option<&[MapiAttachment]> {
        self.messages
            .iter()
            .find(|message| message.folder_id == folder_id && message.id == message_id)
            .map(|message| message.attachments.as_slice())
    }

    pub(crate) fn attachment_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
    ) -> Option<&MapiAttachment> {
        self.attachments_for_message(folder_id, message_id)?
            .iter()
            .find(|attachment| attachment.attach_num == attach_num)
    }

    pub(crate) fn collaboration_folders(&self) -> &[MapiCollaborationFolder] {
        &self.collaboration_folders
    }

    pub(crate) fn collaboration_folder_for_id(
        &self,
        folder_id: u64,
    ) -> Option<&MapiCollaborationFolder> {
        self.collaboration_folders
            .iter()
            .find(|folder| folder.id == folder_id)
    }

    pub(crate) fn contacts_for_folder(&self, folder_id: u64) -> Vec<&MapiContact> {
        self.contacts
            .iter()
            .filter(|contact| contact.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn contact_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiContact> {
        self.contacts
            .iter()
            .find(|contact| contact.folder_id == folder_id && contact.id == item_id)
    }

    pub(crate) fn events_for_folder(&self, folder_id: u64) -> Vec<&MapiEvent> {
        self.events
            .iter()
            .filter(|event| event.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn event_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiEvent> {
        self.events
            .iter()
            .find(|event| event.folder_id == folder_id && event.id == item_id)
    }

    pub(crate) fn permissions_for_folder(&self, folder_id: u64) -> Vec<MapiFolderPermission> {
        let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) else {
            return Vec::new();
        };
        let mut permissions = reserved_permission_rows(folder.canonical_id);
        permissions.extend(
            self.folder_permissions
                .iter()
                .filter(|permission| permission.mailbox_id == folder.canonical_id)
                .cloned(),
        );
        permissions
    }

    pub(crate) fn folder_access_for_principal(
        &self,
        folder_id: u64,
        principal_account_id: Uuid,
    ) -> Option<MapiFolderAccess> {
        let folder = self.folders.iter().find(|folder| folder.id == folder_id)?;
        let permission = self.folder_permissions.iter().find(|permission| {
            permission.mailbox_id == folder.canonical_id
                && permission.member_account_id == Some(principal_account_id)
        })?;
        Some(access_from_rights(permission.rights))
    }

    #[cfg(test)]
    pub(crate) fn folders(&self) -> &[MapiFolder] {
        &self.folders
    }

    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[MapiMessage] {
        &self.messages
    }
}

pub(crate) trait MapiStore: ExchangeStore {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot>;
}

impl<T: ExchangeStore> MapiStore for T {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot> {
        Box::pin(async move {
            let mailboxes = self.fetch_jmap_mailboxes(account_id).await?;
            let query = self
                .query_jmap_email_ids(account_id, None, None, 0, message_limit)
                .await?;
            let emails = self.fetch_jmap_emails(account_id, &query.ids).await?;
            let mut attachments = Vec::with_capacity(emails.len());
            for email in &emails {
                let message_attachments =
                    self.fetch_message_attachments(account_id, email.id).await?;
                attachments.push((email.id, message_attachments));
            }
            let contact_collections = self
                .fetch_accessible_contact_collections(account_id)
                .await?;
            let calendar_collections = self
                .fetch_accessible_calendar_collections(account_id)
                .await?;
            let mut contacts = Vec::new();
            for collection in &contact_collections {
                contacts.extend(
                    self.fetch_accessible_contacts_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let mut events = Vec::new();
            for collection in &calendar_collections {
                events.extend(
                    self.fetch_accessible_events_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let identity_requests = mapi_identity_requests(&mailboxes, &emails, &contacts, &events);
            for identity in self
                .fetch_or_allocate_mapi_identities(account_id, &identity_requests)
                .await?
            {
                crate::mapi::identity::remember_mapi_identity(
                    identity.canonical_id,
                    identity.object_id,
                );
            }
            let mailbox_ids = mailboxes
                .iter()
                .map(|mailbox| mailbox.id)
                .collect::<Vec<_>>();
            let folder_permissions = self
                .fetch_mapi_folder_permissions(account_id, &mailbox_ids)
                .await?;
            Ok(MapiMailStoreSnapshot::new(
                mailboxes,
                emails,
                attachments,
                contact_collections,
                calendar_collections,
                contacts,
                events,
                folder_permissions,
            ))
        })
    }
}

fn mapi_identity_requests(
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    contacts: &[AccessibleContact],
    events: &[AccessibleEvent],
) -> Vec<MapiIdentityRequest> {
    let mut requests = Vec::new();
    requests.extend(mailboxes.iter().map(|mailbox| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Mailbox,
        canonical_id: mailbox.id,
        reserved_global_counter: reserved_folder_counter_for_role(&mailbox.role),
    }));
    requests.extend(emails.iter().map(|email| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Message,
        canonical_id: email.id,
        reserved_global_counter: None,
    }));
    requests.extend(contacts.iter().map(|contact| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Contact,
        canonical_id: contact.id,
        reserved_global_counter: None,
    }));
    requests.extend(events.iter().map(|event| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::CalendarEvent,
        canonical_id: event.id,
        reserved_global_counter: None,
    }));
    requests
}

fn mapi_message_folder_id(email: &JmapEmail, folders: &[MapiFolder]) -> u64 {
    folders
        .iter()
        .find(|folder| folder.canonical_id == email.mailbox_id)
        .map(|folder| folder.id)
        .unwrap_or_else(|| mapi_folder_id_for_role(&email.mailbox_role))
}

fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    reserved_folder_id_for_role(&mailbox.role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&mailbox.id))
        .expect("MAPI folder identity mapping missing")
}

fn mapi_folder_id_for_role(role: &str) -> u64 {
    reserved_folder_id_for_role(role).unwrap_or(crate::mapi::identity::ROOT_FOLDER_ID)
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}

fn mapi_collaboration_folder_id(
    kind: MapiCollaborationFolderKind,
    collection: &CollaborationCollection,
) -> u64 {
    match (kind, collection.id.as_str()) {
        (MapiCollaborationFolderKind::Contacts, "default" | "contacts") => {
            crate::mapi::identity::CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Calendar, "default" | "calendar") => {
            crate::mapi::identity::CALENDAR_FOLDER_ID
        }
        _ => collection
            .id
            .rsplit('-')
            .next()
            .and_then(|value| Uuid::parse_str(value).ok())
            .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
            .unwrap_or_else(|| {
                let seed = match kind {
                    MapiCollaborationFolderKind::Contacts => 17,
                    MapiCollaborationFolderKind::Calendar => 18,
                };
                crate::mapi::identity::mapi_store_id(seed + stable_text_counter(&collection.id))
            }),
    }
}

fn stable_text_counter(value: &str) -> u64 {
    value.bytes().fold(0u64, |acc, byte| {
        acc.wrapping_mul(131).wrapping_add(u64::from(byte))
    }) & 0x0000_FFFF_FFFF_FFFF
}

pub(crate) fn reserved_folder_counter_for_role(role: &str) -> Option<u64> {
    match role {
        "inbox" => Some(crate::mapi::identity::INBOX_FOLDER_COUNTER),
        "drafts" => Some(crate::mapi::identity::DRAFTS_FOLDER_COUNTER),
        "outbox" => Some(crate::mapi::identity::OUTBOX_FOLDER_COUNTER),
        "sent" => Some(crate::mapi::identity::SENT_FOLDER_COUNTER),
        "trash" => Some(crate::mapi::identity::TRASH_FOLDER_COUNTER),
        "contacts" => Some(crate::mapi::identity::CONTACTS_FOLDER_COUNTER),
        "calendar" => Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
        _ => None,
    }
}

fn reserved_folder_id_for_role(role: &str) -> Option<u64> {
    reserved_folder_counter_for_role(role).map(crate::mapi::identity::mapi_store_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_storage::{JmapEmailAddress, JmapEmailMailboxState};

    #[test]
    fn snapshot_projects_canonical_mailbox_message_and_attachment_ids() {
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
        let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
        let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(17),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(18),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "RCA Sync".to_string(),
            sort_order: 10,
            total_emails: 1,
            unread_emails: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "custom".to_string(),
            mailbox_name: "RCA Sync".to_string(),
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "custom".to_string(),
                name: "RCA Sync".to_string(),
                unread: false,
                flagged: false,
                draft: false,
            }],
            received_at: "2026-05-03T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            has_attachments: true,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let attachment = ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: "brief.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 5,
            file_reference: "attachment-ref".to_string(),
        };

        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            vec![(message_id, vec![attachment])],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(snapshot.folders().len(), 1);
        assert_eq!(snapshot.messages().len(), 1);
        assert_eq!(snapshot.messages()[0].canonical_id, message_id);
        assert_eq!(snapshot.messages()[0].folder_id, snapshot.folders()[0].id);
        assert_eq!(
            snapshot.messages()[0].attachments[0].canonical_id,
            attachment_id
        );
        assert_eq!(snapshot.messages()[0].attachments[0].attach_num, 0);
    }
}

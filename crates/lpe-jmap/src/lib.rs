mod blob;
mod calendar;
mod contacts;
mod convert;
mod drafts;
mod error;
mod mail;
mod mailboxes;
mod parse;
mod protocol;
mod service;
mod session;
mod state;
mod store;
mod tasks;
mod upload;
mod vacation;
mod validation;
mod websocket;

pub use crate::service::{router, JmapService};

pub(crate) use crate::convert::resolve_creation_reference;
pub(crate) use crate::parse::parse_submission_email_id;
pub(crate) use crate::service::{
    collection_state_fingerprint, trim_snippet, DEFAULT_GET_LIMIT, JMAP_BLOB_CAPABILITY,
    JMAP_CALENDARS_CAPABILITY, JMAP_CONTACTS_CAPABILITY, JMAP_CORE_CAPABILITY,
    JMAP_MAIL_CAPABILITY, JMAP_SUBMISSION_CAPABILITY, JMAP_TASKS_CAPABILITY,
    JMAP_VACATION_RESPONSE_CAPABILITY, JMAP_WEBSOCKET_CAPABILITY, MAX_BLOB_DATA_SOURCES,
    MAX_CALLS_IN_REQUEST, MAX_CONCURRENT_REQUESTS, MAX_CONCURRENT_UPLOAD, MAX_OBJECTS_IN_GET,
    MAX_OBJECTS_IN_SET, MAX_QUERY_LIMIT, MAX_SIZE_REQUEST, MAX_SIZE_UPLOAD, PUSH_STATE_VERSION,
    QUERY_STATE_VERSION, SESSION_STATE, STATE_TOKEN_VERSION,
};
pub(crate) use crate::session::requested_account_id;
pub(crate) use crate::state::encode_query_state;
pub(crate) use crate::upload::blob_id_for_message;

#[cfg(test)]
use lpe_storage::{
    AuthenticatedAccount, ClientTask, JmapEmail, JmapEmailQuery, JmapMailbox, JmapUploadBlob,
    SieveScriptDocument, SubmitMessageInput, SubmittedMessage,
};
#[cfg(test)]
use serde_json::{json, Value};
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{JmapApiRequest, JmapMethodCall},
        state::{decode_query_state, decode_state, encode_push_state},
        store::{JmapPushListener, JmapStore},
    };
    use anyhow::{anyhow, bail, Result};
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use lpe_storage::mail::parse_rfc822_message;
    use lpe_storage::{
        serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
        AuditEntryInput, CalendarOrganizerMetadata, CalendarParticipantMetadata,
        CalendarParticipantsMetadata, CanonicalChangeCategory, CanonicalChangeReplay,
        CanonicalPushChangeSet, ClientContact, ClientEvent, ClientTaskList,
        CollaborationCollection, CollaborationRights, CreateTaskListInput, JmapEmailAddress,
        JmapEmailSubmission, JmapImportedEmailInput, JmapMailboxCreateInput,
        JmapMailboxUpdateInput, JmapQuota, MailboxAccountAccess, SavedDraftMessage, SenderIdentity,
        UpdateTaskListInput, UpsertClientContactInput, UpsertClientEventInput,
        UpsertClientTaskInput,
    };
    use std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
        time::Instant,
    };

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
        sender_identities: Vec<SenderIdentity>,
        email_submissions: Vec<JmapEmailSubmission>,
        contact_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
        calendar_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
        contacts: Arc<Mutex<Vec<ClientContact>>>,
        events: Arc<Mutex<Vec<ClientEvent>>>,
        task_lists: Arc<Mutex<Vec<ClientTaskList>>>,
        tasks: Arc<Mutex<Vec<ClientTask>>>,
        uploads: Arc<Mutex<Vec<JmapUploadBlob>>>,
        imported_emails: Arc<Mutex<Vec<JmapImportedEmailInput>>>,
        active_sieve_script: Arc<Mutex<Option<String>>>,
        saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
        submitted_drafts: Arc<Mutex<Vec<Uuid>>>,
        submitted_draft_actors: Arc<Mutex<Vec<Uuid>>>,
        submitted_draft_sources: Arc<Mutex<Vec<String>>>,
        canonical_change_cursor: Option<i64>,
        canonical_change_replay: CanonicalChangeReplay,
    }

    struct FakePushListener;

    #[derive(Clone)]
    struct FakeDetector {
        results: Arc<Mutex<Vec<Result<MagikaDetection, String>>>>,
    }

    #[test]
    fn parse_rfc822_message_collects_supported_attachment_parts() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Import\r\n",
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--b1\r\n",
            "Content-Type: application/vnd.oasis.opendocument.text\r\n",
            "Content-Disposition: attachment; filename=\"notes.odt\"\r\n",
            "\r\n",
            "ODT-DATA\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.subject, "Import");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
            self.results
                .lock()
                .unwrap()
                .remove(0)
                .map_err(anyhow::Error::msg)
        }
    }

    fn validator_ok(
        mime_type: &str,
        label: &str,
        extension: &str,
        score: f32,
    ) -> Validator<FakeDetector> {
        validator_sequence(vec![Ok(MagikaDetection {
            label: label.to_string(),
            mime_type: mime_type.to_string(),
            description: label.to_string(),
            group: "document".to_string(),
            extensions: vec![extension.to_string()],
            score: Some(score),
        })])
    }

    fn validator_sequence(
        results: Vec<Result<MagikaDetection, String>>,
    ) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                results: Arc::new(Mutex::new(results)),
            },
            0.80,
        )
    }

    fn validator_error(message: &str) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                results: Arc::new(Mutex::new(vec![Err(message.to_string())])),
            },
            0.80,
        )
    }

    impl JmapPushListener for FakePushListener {
        async fn wait_for_change(
            &mut self,
            _categories: &[CanonicalChangeCategory],
        ) -> Result<CanonicalPushChangeSet> {
            Ok(CanonicalPushChangeSet::default())
        }
    }

    impl FakeStore {
        fn full_rights() -> CollaborationRights {
            CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            }
        }

        fn read_only_rights() -> CollaborationRights {
            CollaborationRights {
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            }
        }

        fn contact_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: "default".to_string(),
                kind: "contacts".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Contacts".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn calendar_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: "default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn accessible_contact(contact: ClientContact) -> AccessibleContact {
            let account = Self::account();
            AccessibleContact {
                id: contact.id,
                collection_id: "default".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                name: contact.name,
                role: contact.role,
                email: contact.email,
                phone: contact.phone,
                team: contact.team,
                notes: contact.notes,
            }
        }

        fn accessible_event(event: ClientEvent) -> AccessibleEvent {
            let account = Self::account();
            AccessibleEvent {
                id: event.id,
                collection_id: "default".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                date: event.date,
                time: event.time,
                time_zone: event.time_zone,
                duration_minutes: event.duration_minutes,
                recurrence_rule: event.recurrence_rule,
                title: event.title,
                location: event.location,
                attendees: event.attendees,
                attendees_json: event.attendees_json,
                notes: event.notes,
            }
        }

        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn draft_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
                role: "drafts".to_string(),
                name: "Drafts".to_string(),
                sort_order: 10,
                total_emails: 1,
                unread_emails: 0,
            }
        }

        fn draft_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::draft_mailbox().id,
                mailbox_role: "drafts".to_string(),
                mailbox_name: "Drafts".to_string(),
                received_at: "2026-04-18T10:00:00Z".to_string(),
                sent_at: None,
                from_address: "alice@example.test".to_string(),
                from_display: Some("Alice".to_string()),
                sender_address: None,
                sender_display: None,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: Self::account().account_id,
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![lpe_storage::JmapEmailAddress {
                    address: "hidden@example.test".to_string(),
                    display_name: None,
                }],
                subject: "Draft subject".to_string(),
                preview: "Draft preview".to_string(),
                body_text: "Draft body".to_string(),
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: 42,
                internet_message_id: Some("<draft@example.test>".to_string()),
                mime_blob_ref: Some(
                    "draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc".to_string(),
                ),
                delivery_status: "draft".to_string(),
            }
        }

        fn inbox_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap(),
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                sort_order: 0,
                total_emails: 1,
                unread_emails: 1,
            }
        }

        fn inbox_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("edededed-eded-eded-eded-edededededed").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::inbox_mailbox().id,
                mailbox_role: "inbox".to_string(),
                mailbox_name: "Inbox".to_string(),
                received_at: "2026-04-19T08:00:00Z".to_string(),
                sent_at: Some("2026-04-19T07:59:00Z".to_string()),
                from_address: "carol@example.test".to_string(),
                from_display: Some("Carol".to_string()),
                sender_address: None,
                sender_display: None,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: Self::account().account_id,
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "alice@example.test".to_string(),
                    display_name: Some("Alice".to_string()),
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Inbox subject".to_string(),
                preview: "Inbox preview".to_string(),
                body_text: "Inbox body".to_string(),
                body_html_sanitized: Some("<p>Inbox body</p>".to_string()),
                unread: true,
                flagged: false,
                has_attachments: false,
                size_octets: 84,
                internet_message_id: Some("<inbox@example.test>".to_string()),
                mime_blob_ref: Some("upload:88888888-8888-8888-8888-888888888888".to_string()),
                delivery_status: "stored".to_string(),
            }
        }

        fn contact() -> ClientContact {
            ClientContact {
                id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
                name: "Bob Example".to_string(),
                role: "Sales".to_string(),
                email: "bob@example.test".to_string(),
                phone: "+33123456789".to_string(),
                team: "North".to_string(),
                notes: "VIP".to_string(),
            }
        }

        fn event() -> ClientEvent {
            ClientEvent {
                id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
                date: "2026-04-20".to_string(),
                time: "09:30".to_string(),
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "bob@example.test".to_string(),
                attendees_json: serialize_calendar_participants_metadata(
                    &CalendarParticipantsMetadata {
                        organizer: Some(CalendarOrganizerMetadata {
                            email: "alice@example.test".to_string(),
                            common_name: "Alice".to_string(),
                        }),
                        attendees: vec![CalendarParticipantMetadata {
                            email: "bob@example.test".to_string(),
                            common_name: "Bob".to_string(),
                            role: "REQ-PARTICIPANT".to_string(),
                            partstat: "tentative".to_string(),
                            rsvp: true,
                        }],
                    },
                ),
                notes: "Daily sync".to_string(),
            }
        }

        fn task() -> ClientTask {
            let account = Self::account();
            ClientTask {
                id: Uuid::parse_str("56565656-5656-5656-5656-565656565656").unwrap(),
                task_list_id: Self::default_task_list().id,
                task_list_sort_order: 0,
                owner_account_id: account.account_id,
                owner_email: account.email,
                owner_display_name: account.display_name,
                is_owned: true,
                rights: CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
                title: "Prepare release".to_string(),
                description: "Confirm the release checklist".to_string(),
                status: "needs-action".to_string(),
                due_at: Some("2026-04-21T09:00:00Z".to_string()),
                completed_at: None,
                sort_order: 10,
                updated_at: "2026-04-20T15:00:00Z".to_string(),
            }
        }

        fn default_task_list() -> ClientTaskList {
            let account = Self::account();
            ClientTaskList {
                id: Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap(),
                name: "Tasks".to_string(),
                role: Some("inbox".to_string()),
                sort_order: 0,
                owner_account_id: account.account_id,
                owner_email: account.email,
                owner_display_name: account.display_name,
                is_owned: true,
                rights: CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
                updated_at: "2026-04-20T15:00:00Z".to_string(),
            }
        }

        fn shared_account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::parse_str("bbbbbbbb-1111-2222-3333-444444444444").unwrap(),
                email: "shared@example.test".to_string(),
                display_name: "Shared Mailbox".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn mailbox_access() -> MailboxAccountAccess {
            let account = Self::account();
            MailboxAccountAccess {
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                is_owned: true,
                may_read: true,
                may_write: true,
                may_send_as: true,
                may_send_on_behalf: true,
            }
        }

        fn shared_mailbox_access(
            may_send_as: bool,
            may_send_on_behalf: bool,
        ) -> MailboxAccountAccess {
            let account = Self::shared_account();
            MailboxAccountAccess {
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                is_owned: false,
                may_read: true,
                may_write: true,
                may_send_as,
                may_send_on_behalf,
            }
        }

        fn shared_mailbox_read_only_access(
            may_send_as: bool,
            may_send_on_behalf: bool,
        ) -> MailboxAccountAccess {
            let mut access = Self::shared_mailbox_access(may_send_as, may_send_on_behalf);
            access.may_write = false;
            access
        }

        fn sender_identity() -> SenderIdentity {
            let account = Self::account();
            SenderIdentity {
                id: format!("self:{}", account.account_id),
                owner_account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                authorization_kind: "self".to_string(),
                sender_address: None,
                sender_display: None,
            }
        }

        fn email_submission() -> JmapEmailSubmission {
            JmapEmailSubmission {
                id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                email_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: Self::draft_email().thread_id,
                identity_id: format!("self:{}", Self::account().account_id),
                identity_email: Self::account().email,
                envelope_mail_from: "alice@example.test".to_string(),
                envelope_rcpt_to: vec!["bob@example.test".to_string()],
                send_at: "2026-04-18T10:01:00Z".to_string(),
                undo_status: "final".to_string(),
                delivery_status: "queued".to_string(),
            }
        }
    }

    fn push_subscription(
        enabled_types: HashSet<String>,
        last_type_states: HashMap<String, HashMap<String, String>>,
    ) -> crate::websocket::PushSubscription {
        crate::websocket::PushSubscription {
            enabled_types,
            last_push_state: Some(encode_push_state(&last_type_states, None).unwrap()),
            last_type_states,
            last_journal_cursor: None,
        }
    }

    impl JmapStore for FakeStore {
        type PushListener = FakePushListener;

        async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
            Ok(if token == "token" {
                self.session.clone()
            } else {
                None
            })
        }

        async fn create_push_listener(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Self::PushListener> {
            Ok(FakePushListener)
        }

        async fn fetch_canonical_change_cursor(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Option<i64>> {
            Ok(self.canonical_change_cursor)
        }

        async fn replay_canonical_changes(
            &self,
            _principal_account_id: Uuid,
            _after_cursor: i64,
            _categories: &[CanonicalChangeCategory],
            _max_rows: u64,
        ) -> Result<CanonicalChangeReplay> {
            Ok(self.canonical_change_replay.clone())
        }

        async fn fetch_jmap_mailboxes(&self, _account_id: Uuid) -> Result<Vec<JmapMailbox>> {
            Ok(self.mailboxes.clone())
        }

        async fn fetch_accessible_mailbox_accounts(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<MailboxAccountAccess>> {
            if self.accessible_mailbox_accounts.is_empty() {
                Ok(vec![Self::mailbox_access()])
            } else {
                Ok(self.accessible_mailbox_accounts.clone())
            }
        }

        async fn fetch_sender_identities(
            &self,
            _principal_account_id: Uuid,
            target_account_id: Uuid,
        ) -> Result<Vec<SenderIdentity>> {
            let identities = if self.sender_identities.is_empty() {
                vec![Self::sender_identity()]
            } else {
                self.sender_identities.clone()
            };
            Ok(identities
                .into_iter()
                .filter(|identity| identity.owner_account_id == target_account_id)
                .collect())
        }

        async fn fetch_jmap_mailbox_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.mailboxes.iter().map(|mailbox| mailbox.id).collect())
        }

        async fn query_jmap_email_ids(
            &self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&str>,
            position: u64,
            limit: u64,
        ) -> Result<JmapEmailQuery> {
            let mut ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.id)
                .collect::<Vec<_>>();
            let total = ids.len() as u64;
            ids = ids
                .into_iter()
                .skip(position as usize)
                .take(limit as usize)
                .collect();
            Ok(JmapEmailQuery { ids, total })
        }

        async fn fetch_all_jmap_email_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.emails.iter().map(|email| email.id).collect())
        }

        async fn fetch_all_jmap_thread_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self
                .emails
                .iter()
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect())
        }

        async fn query_jmap_thread_ids(
            &self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&str>,
            position: u64,
            limit: u64,
        ) -> Result<lpe_storage::JmapThreadQuery> {
            let mut ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            ids.sort();
            ids.reverse();
            let total = ids.len() as u64;
            ids = ids
                .into_iter()
                .skip(position as usize)
                .take(limit as usize)
                .collect();
            Ok(lpe_storage::JmapThreadQuery { ids, total })
        }

        async fn create_jmap_mailbox(
            &self,
            input: JmapMailboxCreateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                role: "".to_string(),
                name: input.name,
                sort_order: input.sort_order.unwrap_or(99),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn update_jmap_mailbox(
            &self,
            input: JmapMailboxUpdateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: input.mailbox_id,
                role: "".to_string(),
                name: input.name.unwrap_or_else(|| "Updated".to_string()),
                sort_order: input.sort_order.unwrap_or(10),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn destroy_jmap_mailbox(
            &self,
            _account_id: Uuid,
            _mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn fetch_jmap_emails(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmail>> {
            Ok(ids
                .iter()
                .filter_map(|id| self.emails.iter().find(|email| email.id == *id).cloned())
                .collect())
        }

        async fn fetch_jmap_draft(&self, _account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
            Ok(self.emails.iter().find(|email| email.id == id).cloned())
        }

        async fn fetch_jmap_email_submissions(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmailSubmission>> {
            let submissions = if self.email_submissions.is_empty() {
                vec![FakeStore::email_submission()]
            } else {
                self.email_submissions.clone()
            };
            if ids.is_empty() {
                Ok(submissions)
            } else {
                Ok(submissions
                    .into_iter()
                    .filter(|submission| ids.contains(&submission.id))
                    .collect())
            }
        }

        async fn fetch_jmap_quota(&self, _account_id: Uuid) -> Result<JmapQuota> {
            Ok(JmapQuota {
                id: "mail".to_string(),
                name: "Mail".to_string(),
                used: 10,
                hard_limit: 100,
            })
        }

        async fn fetch_active_sieve_script(
            &self,
            _account_id: Uuid,
        ) -> Result<Option<SieveScriptDocument>> {
            Ok(self
                .active_sieve_script
                .lock()
                .unwrap()
                .as_ref()
                .map(|content| SieveScriptDocument {
                    name: "active".to_string(),
                    content: content.clone(),
                    is_active: true,
                    updated_at: "2026-04-20T15:00:00Z".to_string(),
                }))
        }

        async fn put_sieve_script(
            &self,
            _account_id: Uuid,
            _name: &str,
            content: &str,
            activate: bool,
            _audit: AuditEntryInput,
        ) -> Result<SieveScriptDocument> {
            if activate {
                *self.active_sieve_script.lock().unwrap() = Some(content.to_string());
            }
            Ok(SieveScriptDocument {
                name: "jmap-vacation".to_string(),
                content: content.to_string(),
                is_active: activate,
                updated_at: "2026-04-20T15:00:00Z".to_string(),
            })
        }

        async fn set_active_sieve_script(
            &self,
            _account_id: Uuid,
            name: Option<&str>,
            _audit: AuditEntryInput,
        ) -> Result<Option<String>> {
            if name.is_none() {
                *self.active_sieve_script.lock().unwrap() = None;
            }
            Ok(name.map(ToString::to_string))
        }

        async fn save_jmap_upload_blob(
            &self,
            account_id: Uuid,
            media_type: &str,
            blob_bytes: &[u8],
        ) -> Result<JmapUploadBlob> {
            let mut uploads = self.uploads.lock().unwrap();
            let stable_first_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
            let id = if uploads.iter().any(|blob| blob.id == stable_first_id) {
                Uuid::new_v4()
            } else {
                stable_first_id
            };
            let blob = JmapUploadBlob {
                id,
                account_id,
                media_type: media_type.to_string(),
                octet_size: blob_bytes.len() as u64,
                blob_bytes: blob_bytes.to_vec(),
            };
            uploads.push(blob.clone());
            Ok(blob)
        }

        async fn fetch_jmap_upload_blob(
            &self,
            _account_id: Uuid,
            blob_id: Uuid,
        ) -> Result<Option<JmapUploadBlob>> {
            Ok(self
                .uploads
                .lock()
                .unwrap()
                .iter()
                .find(|blob| blob.id == blob_id)
                .cloned())
        }

        async fn save_draft_message(
            &self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> Result<SavedDraftMessage> {
            self.saved_drafts.lock().unwrap().push(input.clone());
            Ok(SavedDraftMessage {
                message_id: input.draft_message_id.unwrap_or_else(Uuid::new_v4),
                account_id: input.account_id,
                submitted_by_account_id: input.submitted_by_account_id,
                draft_mailbox_id: FakeStore::draft_mailbox().id,
                delivery_status: "draft".to_string(),
            })
        }

        async fn delete_draft_message(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn submit_draft_message(
            &self,
            account_id: Uuid,
            draft_message_id: Uuid,
            submitted_by_account_id: Uuid,
            source: &str,
            _audit: AuditEntryInput,
        ) -> Result<SubmittedMessage> {
            self.submitted_drafts.lock().unwrap().push(draft_message_id);
            self.submitted_draft_actors
                .lock()
                .unwrap()
                .push(submitted_by_account_id);
            self.submitted_draft_sources
                .lock()
                .unwrap()
                .push(source.to_string());
            Ok(SubmittedMessage {
                message_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                account_id,
                submitted_by_account_id,
                sent_mailbox_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                outbound_queue_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                delivery_status: "queued".to_string(),
            })
        }

        async fn copy_jmap_email(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            target_mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            let mut email = FakeStore::draft_email();
            email.id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
            email.mailbox_id = target_mailbox_id;
            email.mailbox_role = "".to_string();
            email.mailbox_name = "Archive".to_string();
            Ok(email)
        }

        async fn import_jmap_email(
            &self,
            input: JmapImportedEmailInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            self.imported_emails.lock().unwrap().push(input.clone());
            Ok(JmapEmail {
                id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
                thread_id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
                mailbox_id: input.mailbox_id,
                mailbox_role: "".to_string(),
                mailbox_name: "Imported".to_string(),
                received_at: "2026-04-18T10:05:00Z".to_string(),
                sent_at: None,
                from_address: input.from_address,
                from_display: input.from_display,
                sender_address: input.sender_address,
                sender_display: input.sender_display,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: input.submitted_by_account_id,
                to: input
                    .to
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                cc: input
                    .cc
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                bcc: Vec::new(),
                subject: input.subject,
                preview: "Imported".to_string(),
                body_text: input.body_text,
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: input.size_octets,
                internet_message_id: input.internet_message_id,
                mime_blob_ref: Some(input.mime_blob_ref),
                delivery_status: "stored".to_string(),
            })
        }

        async fn fetch_accessible_contact_collections(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<CollaborationCollection>> {
            let collections = self.contact_collections.lock().unwrap();
            if collections.is_empty() {
                Ok(vec![Self::contact_collection()])
            } else {
                Ok(collections.clone())
            }
        }

        async fn fetch_accessible_contacts(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<AccessibleContact>> {
            Ok(self
                .contacts
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .map(Self::accessible_contact)
                .collect())
        }

        async fn fetch_accessible_contacts_by_ids(
            &self,
            _principal_account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<AccessibleContact>> {
            Ok(self
                .contacts
                .lock()
                .unwrap()
                .iter()
                .filter(|contact| ids.contains(&contact.id))
                .cloned()
                .map(Self::accessible_contact)
                .collect())
        }

        async fn create_accessible_contact(
            &self,
            _principal_account_id: Uuid,
            _collection_id: Option<&str>,
            input: UpsertClientContactInput,
        ) -> Result<AccessibleContact> {
            let contact = ClientContact {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            };
            let mut contacts = self.contacts.lock().unwrap();
            contacts.retain(|entry| entry.id != contact.id);
            contacts.push(contact.clone());
            Ok(Self::accessible_contact(contact))
        }

        async fn update_accessible_contact(
            &self,
            principal_account_id: Uuid,
            contact_id: Uuid,
            mut input: UpsertClientContactInput,
        ) -> Result<AccessibleContact> {
            input.id = Some(contact_id);
            self.create_accessible_contact(principal_account_id, Some("default"), input)
                .await
        }

        async fn delete_accessible_contact(
            &self,
            _principal_account_id: Uuid,
            contact_id: Uuid,
        ) -> Result<()> {
            let mut contacts = self.contacts.lock().unwrap();
            let original_len = contacts.len();
            contacts.retain(|entry| entry.id != contact_id);
            if contacts.len() == original_len {
                bail!("contact not found");
            }
            Ok(())
        }

        async fn fetch_accessible_calendar_collections(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<CollaborationCollection>> {
            let collections = self.calendar_collections.lock().unwrap();
            if collections.is_empty() {
                Ok(vec![Self::calendar_collection()])
            } else {
                Ok(collections.clone())
            }
        }

        async fn fetch_accessible_events(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<AccessibleEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .map(Self::accessible_event)
                .collect())
        }

        async fn fetch_accessible_events_by_ids(
            &self,
            _principal_account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<AccessibleEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| ids.contains(&event.id))
                .cloned()
                .map(Self::accessible_event)
                .collect())
        }

        async fn create_accessible_event(
            &self,
            _principal_account_id: Uuid,
            _collection_id: Option<&str>,
            input: UpsertClientEventInput,
        ) -> Result<AccessibleEvent> {
            let event = ClientEvent {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
            };
            let mut events = self.events.lock().unwrap();
            events.retain(|entry| entry.id != event.id);
            events.push(event.clone());
            Ok(Self::accessible_event(event))
        }

        async fn update_accessible_event(
            &self,
            principal_account_id: Uuid,
            event_id: Uuid,
            mut input: UpsertClientEventInput,
        ) -> Result<AccessibleEvent> {
            input.id = Some(event_id);
            self.create_accessible_event(principal_account_id, Some("default"), input)
                .await
        }

        async fn delete_accessible_event(
            &self,
            _principal_account_id: Uuid,
            event_id: Uuid,
        ) -> Result<()> {
            let mut events = self.events.lock().unwrap();
            let original_len = events.len();
            events.retain(|entry| entry.id != event_id);
            if events.len() == original_len {
                bail!("event not found");
            }
            Ok(())
        }

        async fn fetch_jmap_task_lists(&self, _account_id: Uuid) -> Result<Vec<ClientTaskList>> {
            let task_lists = self.task_lists.lock().unwrap();
            if task_lists.is_empty() {
                Ok(vec![Self::default_task_list()])
            } else {
                Ok(task_lists.clone())
            }
        }

        async fn fetch_jmap_task_lists_by_ids(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<ClientTaskList>> {
            Ok(self
                .fetch_jmap_task_lists(Self::account().account_id)
                .await?
                .into_iter()
                .filter(|task_list| ids.contains(&task_list.id))
                .collect())
        }

        async fn create_jmap_task_list(
            &self,
            input: CreateTaskListInput,
        ) -> Result<ClientTaskList> {
            let task_list = ClientTaskList {
                id: Uuid::new_v4(),
                name: input.name.trim().to_string(),
                role: None,
                sort_order: input.sort_order,
                owner_account_id: Self::account().account_id,
                owner_email: Self::account().email,
                owner_display_name: Self::account().display_name,
                is_owned: true,
                rights: CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
                updated_at: "2026-04-20T15:30:00Z".to_string(),
            };
            let mut task_lists = self.task_lists.lock().unwrap();
            task_lists.push(task_list.clone());
            Ok(task_list)
        }

        async fn update_jmap_task_list(
            &self,
            input: UpdateTaskListInput,
        ) -> Result<ClientTaskList> {
            let mut task_lists = self.task_lists.lock().unwrap();
            let task_list = task_lists
                .iter_mut()
                .find(|task_list| task_list.id == input.task_list_id)
                .ok_or_else(|| anyhow!("task list not found"))?;
            if let Some(name) = input.name {
                task_list.name = name;
            }
            if let Some(sort_order) = input.sort_order {
                task_list.sort_order = sort_order;
            }
            task_list.updated_at = "2026-04-20T16:00:00Z".to_string();
            Ok(task_list.clone())
        }

        async fn delete_jmap_task_list(&self, _account_id: Uuid, task_list_id: Uuid) -> Result<()> {
            let mut task_lists = self.task_lists.lock().unwrap();
            if task_lists
                .iter()
                .any(|task_list| task_list.id == task_list_id && task_list.role.is_some())
            {
                bail!("default task list cannot be destroyed");
            }
            if self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .any(|task| task.task_list_id == task_list_id)
            {
                bail!("task list must be empty before it can be destroyed");
            }
            task_lists.retain(|task_list| task_list.id != task_list_id);
            Ok(())
        }

        async fn fetch_jmap_tasks(&self, _account_id: Uuid) -> Result<Vec<ClientTask>> {
            Ok(self.tasks.lock().unwrap().clone())
        }

        async fn fetch_jmap_tasks_by_ids(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<ClientTask>> {
            Ok(self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .filter(|task| ids.contains(&task.id))
                .cloned()
                .collect())
        }

        async fn upsert_jmap_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask> {
            let task_id = input.id.unwrap_or_else(Uuid::new_v4);
            let task_list_id = input
                .task_list_id
                .unwrap_or_else(|| Self::default_task_list().id);
            let task_list = self
                .fetch_jmap_task_lists(Self::account().account_id)
                .await?
                .into_iter()
                .find(|task_list| task_list.id == task_list_id)
                .ok_or_else(|| anyhow!("task list not found"))?;
            if !task_list.rights.may_write {
                bail!("write access is not granted on this task list");
            }

            let existing_task = self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .find(|task| task.id == task_id)
                .cloned();
            if let Some(existing_task) = existing_task.as_ref() {
                if !existing_task.rights.may_write {
                    bail!("write access is not granted on this task");
                }
            }

            let task = ClientTask {
                id: task_id,
                task_list_id,
                task_list_sort_order: task_list.sort_order,
                owner_account_id: task_list.owner_account_id,
                owner_email: task_list.owner_email,
                owner_display_name: task_list.owner_display_name,
                is_owned: task_list.is_owned,
                rights: task_list.rights.clone(),
                title: input.title.trim().to_string(),
                description: input.description.trim().to_string(),
                status: input.status.trim().to_ascii_lowercase(),
                due_at: input.due_at,
                completed_at: if input.status.trim().eq_ignore_ascii_case("completed") {
                    input
                        .completed_at
                        .or_else(|| Some("2026-04-20T16:00:00Z".to_string()))
                } else {
                    None
                },
                sort_order: input.sort_order,
                updated_at: if input.id.is_some() {
                    "2026-04-20T16:00:00Z".to_string()
                } else {
                    "2026-04-20T15:30:00Z".to_string()
                },
            };
            let mut tasks = self.tasks.lock().unwrap();
            tasks.retain(|entry| entry.id != task.id);
            tasks.push(task.clone());
            Ok(task)
        }

        async fn delete_jmap_task(&self, _account_id: Uuid, task_id: Uuid) -> Result<()> {
            let mut tasks = self.tasks.lock().unwrap();
            let task = tasks
                .iter()
                .find(|entry| entry.id == task_id)
                .cloned()
                .ok_or_else(|| anyhow!("task not found"))?;
            if !task.rights.may_delete {
                bail!("delete access is not granted on this task");
            }
            let original_len = tasks.len();
            tasks.retain(|entry| entry.id != task_id);
            if tasks.len() == original_len {
                bail!("task not found");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn session_uses_existing_account_authentication() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(
                Some("Bearer token"),
                Some("wss://mail.example.test/jmap/ws"),
                None,
            )
            .await
            .unwrap();

        assert_eq!(session.username, "alice@example.test");
        assert_eq!(session.api_url, "/jmap/api");
        assert!(session.capabilities.contains_key(JMAP_MAIL_CAPABILITY));
        assert_eq!(
            session.capabilities[JMAP_CORE_CAPABILITY]["maxSizeUpload"],
            MAX_SIZE_UPLOAD
        );
        assert_eq!(
            session.capabilities[JMAP_CORE_CAPABILITY]["maxSizeRequest"],
            MAX_SIZE_REQUEST
        );
        assert_eq!(
            session.capabilities[JMAP_CORE_CAPABILITY]["maxConcurrentRequests"],
            MAX_CONCURRENT_REQUESTS
        );
        assert_eq!(
            session.capabilities[JMAP_CORE_CAPABILITY]["maxCallsInRequest"],
            MAX_CALLS_IN_REQUEST
        );
        assert_eq!(
            session.capabilities[JMAP_CORE_CAPABILITY]["maxConcurrentUpload"],
            MAX_CONCURRENT_UPLOAD
        );
        assert_eq!(
            session.capabilities[JMAP_WEBSOCKET_CAPABILITY]["url"],
            "wss://mail.example.test/jmap/ws"
        );
        assert_eq!(session.event_source_url, "");
    }

    #[tokio::test]
    async fn session_urls_respect_forwarded_jmap_prefix() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(
                Some("Bearer token"),
                Some("wss://mail.example.test/api/jmap/ws"),
                Some("/api/jmap"),
            )
            .await
            .unwrap();

        assert_eq!(session.api_url, "/api/jmap/api");
        assert_eq!(session.upload_url, "/api/jmap/upload/{accountId}");
        assert_eq!(
            session.download_url,
            "/api/jmap/download/{accountId}/{blobId}/{name}"
        );

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-prefix", "/api/jmap".parse().unwrap());
        assert_eq!(
            crate::session::websocket_url(&headers).unwrap(),
            "wss://mail.example.test/api/jmap/ws"
        );
    }

    #[tokio::test]
    async fn session_state_tracks_accessible_mailbox_projection() {
        let base = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
            ..Default::default()
        });
        let delegated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let read_only_without_sender = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(false, false),
            ],
            ..Default::default()
        });
        let read_only_with_sender = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(true, false),
            ],
            ..Default::default()
        });

        let base = base
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        let delegated = delegated
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        let read_only_without_sender = read_only_without_sender
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        let read_only_with_sender = read_only_with_sender
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();

        assert_ne!(base.state, delegated.state);
        assert_eq!(read_only_without_sender.state, read_only_with_sender.state);
        assert!(!delegated.state.contains("shared@example.test"));

        let api = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        })
        .handle_api_request(
            Some("Bearer token"),
            JmapApiRequest {
                using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                method_calls: vec![JmapMethodCall(
                    "Mailbox/query".to_string(),
                    json!({}),
                    "c1".to_string(),
                )],
            },
        )
        .await
        .unwrap();
        assert_eq!(api.session_state, delegated.state);
    }

    #[tokio::test]
    async fn session_and_identity_include_accessible_shared_mailbox_accounts() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, true),
            ],
            sender_identities: vec![
                FakeStore::sender_identity(),
                SenderIdentity {
                    id: format!("send-as:{}", FakeStore::shared_account().account_id),
                    owner_account_id: FakeStore::shared_account().account_id,
                    email: FakeStore::shared_account().email,
                    display_name: FakeStore::shared_account().display_name,
                    authorization_kind: "send-as".to_string(),
                    sender_address: None,
                    sender_display: None,
                },
                SenderIdentity {
                    id: format!("send-on-behalf:{}", FakeStore::shared_account().account_id),
                    owner_account_id: FakeStore::shared_account().account_id,
                    email: FakeStore::shared_account().email,
                    display_name: FakeStore::shared_account().display_name,
                    authorization_kind: "send-on-behalf".to_string(),
                    sender_address: Some(FakeStore::account().email),
                    sender_display: Some(FakeStore::account().display_name),
                },
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let session = service
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        assert!(session
            .accounts
            .contains_key(&FakeStore::shared_account().account_id.to_string()));
        assert_eq!(
            session.accounts[&FakeStore::shared_account().account_id.to_string()].is_read_only,
            false
        );
        let shared_capabilities = &session.accounts
            [&FakeStore::shared_account().account_id.to_string()]
            .account_capabilities;
        assert!(shared_capabilities.contains_key(JMAP_MAIL_CAPABILITY));
        assert!(shared_capabilities.contains_key(JMAP_BLOB_CAPABILITY));
        assert_eq!(
            shared_capabilities[JMAP_BLOB_CAPABILITY]["maxSizeBlobSet"],
            MAX_SIZE_UPLOAD
        );
        assert!(shared_capabilities.contains_key(JMAP_SUBMISSION_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_CONTACTS_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_CALENDARS_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_TASKS_CAPABILITY));

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "properties": [
                                "id",
                                "email",
                                "xLpeOwnerAccountId",
                                "xLpeAuthorizationKind",
                                "xLpeSender"
                            ]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeAuthorizationKind"],
            Value::String("send-on-behalf".to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeOwnerAccountId"],
            Value::String(FakeStore::shared_account().account_id.to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeSender"]["email"],
            Value::String("alice@example.test".to_string())
        );
    }

    #[tokio::test]
    async fn session_omits_submission_for_shared_mailbox_without_sender_grant() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });

        let session = service
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        let shared_capabilities = &session.accounts
            [&FakeStore::shared_account().account_id.to_string()]
            .account_capabilities;

        assert!(shared_capabilities.contains_key(JMAP_MAIL_CAPABILITY));
        assert!(shared_capabilities.contains_key(JMAP_BLOB_CAPABILITY));
        assert_eq!(
            shared_capabilities[JMAP_BLOB_CAPABILITY]["maxSizeBlobSet"],
            MAX_SIZE_UPLOAD
        );
        assert!(!shared_capabilities.contains_key(JMAP_SUBMISSION_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_CONTACTS_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_CALENDARS_CAPABILITY));
        assert!(!shared_capabilities.contains_key(JMAP_TASKS_CAPABILITY));
    }

    #[tokio::test]
    async fn email_submission_get_hides_shared_account_without_submit_rights() {
        let submission = FakeStore::email_submission();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            email_submissions: vec![submission.clone()],
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "EmailSubmission/get".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "ids": [submission.id.to_string()]
                            }),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "EmailSubmission/changes".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "sinceState": "0"
                            }),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(response.method_responses[0].1["list"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            response.method_responses[0].1["notFound"],
            json!([submission.id.to_string()])
        );
        assert!(response.method_responses[1].1["created"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn session_omits_submission_for_read_only_shared_mailbox_with_sender_grant() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(true, false),
            ],
            sender_identities: vec![SenderIdentity {
                id: format!("send-as:{}", FakeStore::shared_account().account_id),
                owner_account_id: FakeStore::shared_account().account_id,
                email: FakeStore::shared_account().email,
                display_name: FakeStore::shared_account().display_name,
                authorization_kind: "send-as".to_string(),
                sender_address: None,
                sender_display: None,
            }],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let session = service
            .session_document(Some("Bearer token"), None, None)
            .await
            .unwrap();
        let shared_account = &session.accounts[&FakeStore::shared_account().account_id.to_string()];
        assert!(shared_account.is_read_only);
        assert!(shared_account
            .account_capabilities
            .contains_key(JMAP_MAIL_CAPABILITY));
        assert!(shared_account
            .account_capabilities
            .contains_key(JMAP_BLOB_CAPABILITY));
        assert_eq!(
            shared_account.account_capabilities[JMAP_BLOB_CAPABILITY]["maxSizeBlobSet"],
            0
        );
        assert!(!shared_account
            .account_capabilities
            .contains_key(JMAP_SUBMISSION_CAPABILITY));

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string()
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        assert!(response.method_responses[0].1["list"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn mailbox_get_projects_delegated_submit_rights_from_sender_grants() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "ids": [FakeStore::draft_mailbox().id.to_string()],
                            "properties": ["id", "myRights"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let rights = &response.method_responses[0].1["list"][0]["myRights"];
        assert_eq!(rights["mayReadItems"], true);
        assert_eq!(rights["mayAddItems"], false);
        assert_eq!(rights["maySubmit"], false);

        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);
        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "ids": [FakeStore::draft_mailbox().id.to_string()],
                            "properties": ["id", "myRights"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let rights = &response.method_responses[0].1["list"][0]["myRights"];
        assert_eq!(rights["mayAddItems"], true);
        assert_eq!(rights["maySubmit"], true);

        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(true, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);
        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "ids": [FakeStore::draft_mailbox().id.to_string()],
                            "properties": ["id", "myRights"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let rights = &response.method_responses[0].1["list"][0]["myRights"];
        assert_eq!(rights["mayAddItems"], false);
        assert_eq!(rights["maySubmit"], false);
    }

    #[tokio::test]
    async fn mailbox_state_does_not_advertise_submit_for_read_only_sender_grant() {
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(false, false),
            ],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string()
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let state = initial_response.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let with_sender_grant = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(true, false),
            ],
            ..Default::default()
        });
        let changes = with_sender_grant
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/changes".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "sinceState": state
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(changes.method_responses[0].1["created"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(changes.method_responses[0].1["updated"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(changes.method_responses[0].1["destroyed"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn mailbox_changes_report_delegated_submit_right_changes() {
        let shared = FakeStore::shared_account();
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({"accountId": shared.account_id.to_string()}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let prior_state = initial_response.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            ..Default::default()
        });
        let changes = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/changes".to_string(),
                        json!({
                            "accountId": shared.account_id.to_string(),
                            "sinceState": prior_state
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let updated = changes.method_responses[0].1["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 1);
        assert_eq!(
            updated[0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
    }

    #[tokio::test]
    async fn email_set_creates_draft_through_canonical_storage() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "bcc": [{"email": "hidden@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].from_address, "alice@example.test");
        assert_eq!(saved[0].bcc.len(), 1);
        assert!(response.created_ids.contains_key("k1"));
    }

    #[tokio::test]
    async fn email_set_creates_delegated_shared_mailbox_draft() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, true),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test", "name": "Shared Mailbox"}],
                                    "sender": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "Delegated",
                                    "textBody": "Shared draft"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved[0].account_id, FakeStore::shared_account().account_id);
        assert_eq!(
            saved[0].submitted_by_account_id,
            FakeStore::account().account_id
        );
        assert_eq!(saved[0].from_address, "shared@example.test");
        assert_eq!(
            saved[0].sender_address.as_deref(),
            Some("alice@example.test")
        );
    }

    #[tokio::test]
    async fn email_set_rejects_shared_mailbox_draft_without_sender_delegation() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "No sender rights",
                                    "textBody": "Denied"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(store.saved_drafts.lock().unwrap().len(), 0);
        assert_eq!(
            response.method_responses[0].1["notCreated"]["k1"]["description"],
            "sender delegation is required to write drafts in this mailbox account"
        );
    }

    #[tokio::test]
    async fn email_set_rejects_inaccessible_account_id() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "No access",
                                    "textBody": "Denied"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
    }

    #[tokio::test]
    async fn email_set_rejects_read_only_shared_mailbox_mutations() {
        let read_only = FakeStore::shared_mailbox_read_only_access(true, false);
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access(), read_only],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "No write",
                                    "textBody": "Denied"
                                }
                            },
                            "update": {
                                FakeStore::draft_email().id.to_string(): {
                                    "subject": "Still denied"
                                }
                            },
                            "destroy": [FakeStore::draft_email().id.to_string()]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(store.saved_drafts.lock().unwrap().len(), 0);
        assert_eq!(
            response.method_responses[0].1["notCreated"]["k1"]["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[0].1["notUpdated"][FakeStore::draft_email().id.to_string()]
                ["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[0].1["notDestroyed"][FakeStore::draft_email().id.to_string()]
                ["description"],
            "write access is not granted on this mailbox account"
        );
    }

    #[tokio::test]
    async fn email_set_maps_seen_and_flagged_keywords_to_draft_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body",
                                    "keywords": {
                                        "$draft": true,
                                        "$seen": true,
                                        "$flagged": true
                                    }
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].unread, Some(false));
        assert_eq!(saved[0].flagged, Some(true));
    }

    #[tokio::test]
    async fn email_submission_set_submits_existing_draft_and_returns_queued_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let submitted = store.submitted_drafts.lock().unwrap();
        assert_eq!(submitted.as_slice(), &[FakeStore::draft_email().id]);
        assert_eq!(
            store.submitted_draft_sources.lock().unwrap().as_slice(),
            ["jmap"]
        );
        assert_eq!(
            store.submitted_draft_actors.lock().unwrap().as_slice(),
            [FakeStore::account().account_id]
        );
        assert!(store.saved_drafts.lock().unwrap().is_empty());
        let payload = &response.method_responses[0].1;
        assert_eq!(
            decode_state(payload["oldState"].as_str().unwrap())
                .unwrap()
                .kind,
            "EmailSubmission"
        );
        assert_eq!(
            decode_state(payload["newState"].as_str().unwrap())
                .unwrap()
                .kind,
            "EmailSubmission"
        );
        assert_eq!(
            payload["created"]["send1"]["id"],
            Value::String("11111111-2222-3333-4444-555555555555".to_string())
        );
        assert_eq!(
            payload["created"]["send1"]["undoStatus"],
            Value::String("final".to_string())
        );
    }

    #[tokio::test]
    async fn email_submission_set_uses_authenticated_submitter_for_delegated_draft() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.submitted_draft_actors.lock().unwrap().as_slice(),
            [FakeStore::account().account_id]
        );
        assert_eq!(
            response.method_responses[0].1["created"]["send1"]["emailId"],
            Value::String("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee".to_string())
        );
    }

    #[tokio::test]
    async fn email_submission_set_rejects_delegated_submit_without_sender_grant() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(store.submitted_drafts.lock().unwrap().is_empty());
        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert!(response.method_responses[0].1["description"]
            .as_str()
            .unwrap()
            .contains("sender delegation is required"));
    }

    #[tokio::test]
    async fn email_submission_set_rejects_read_only_shared_mailbox_draft_submit() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(true, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(store.submitted_drafts.lock().unwrap().is_empty());
        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert!(response.method_responses[0].1["description"]
            .as_str()
            .unwrap()
            .contains("write access is required"));
    }

    #[tokio::test]
    async fn email_get_only_returns_bcc_for_explicit_owner_draft_request() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email(), FakeStore::inbox_email()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({
                            "ids": [
                                FakeStore::draft_email().id.to_string(),
                                FakeStore::inbox_email().id.to_string()
                            ],
                            "properties": ["id", "bcc"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let list = response.method_responses[0].1["list"].as_array().unwrap();
        assert_eq!(
            list[0]["bcc"][0]["email"],
            Value::String("hidden@example.test".to_string())
        );
        assert_eq!(list[1]["bcc"], Value::Null);
    }

    #[tokio::test]
    async fn email_get_hides_bcc_for_delegated_shared_mailbox_projection() {
        let shared = FakeStore::shared_account();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({
                            "accountId": shared.account_id.to_string(),
                            "ids": [FakeStore::draft_email().id.to_string()],
                            "properties": ["id", "bcc"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let list = response.method_responses[0].1["list"].as_array().unwrap();
        assert_eq!(list[0]["bcc"], Value::Null);
    }

    #[tokio::test]
    async fn delegated_email_and_thread_states_ignore_bcc_only_changes() {
        let mut visible_email = FakeStore::draft_email();
        visible_email.bcc.clear();
        let hidden_bcc_email = FakeStore::draft_email();
        let without_bcc = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![visible_email],
            ..Default::default()
        });
        let with_bcc = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![hidden_bcc_email],
            ..Default::default()
        });
        let mut owner_access = FakeStore::shared_mailbox_access(true, true);
        owner_access.is_owned = true;
        let delegated_access = FakeStore::shared_mailbox_access(false, false);

        let owner_email_without = without_bcc
            .mail_object_state(&owner_access, "Email")
            .await
            .unwrap();
        let owner_email_with = with_bcc
            .mail_object_state(&owner_access, "Email")
            .await
            .unwrap();
        assert_ne!(owner_email_without, owner_email_with);

        let delegated_email_without = without_bcc
            .mail_object_state(&delegated_access, "Email")
            .await
            .unwrap();
        let delegated_email_with = with_bcc
            .mail_object_state(&delegated_access, "Email")
            .await
            .unwrap();
        assert_eq!(delegated_email_without, delegated_email_with);

        let delegated_thread_without = without_bcc
            .mail_object_state(&delegated_access, "Thread")
            .await
            .unwrap();
        let delegated_thread_with = with_bcc
            .mail_object_state(&delegated_access, "Thread")
            .await
            .unwrap();
        assert_eq!(delegated_thread_without, delegated_thread_with);
    }

    #[tokio::test]
    async fn email_state_tokens_do_not_expose_message_or_bcc_content() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({"properties": ["id", "bcc", "subject", "bodyValues"]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["bcc"][0]["email"],
            Value::String("hidden@example.test".to_string())
        );
        let token = response.method_responses[0].1["state"].as_str().unwrap();
        let decoded = decode_state(token).unwrap();
        let fingerprints = decoded
            .entries
            .iter()
            .map(|entry| entry.fingerprint.as_str())
            .collect::<Vec<_>>()
            .join("|");

        assert!(!token.contains("hidden@example.test"));
        assert!(!fingerprints.contains("hidden@example.test"));
        assert!(!fingerprints.contains("Draft subject"));
        assert!(!fingerprints.contains("Draft body"));
        assert!(fingerprints
            .chars()
            .all(|value| value.is_ascii_hexdigit() || value == '|'));
    }

    #[tokio::test]
    async fn mailbox_and_email_changes_return_existing_ids_from_initial_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn changes_reject_malformed_and_cross_account_state_tokens() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let owned_state = initial.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({"sinceState": "not-a-state"}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({
                                "accountId": shared.account_id.to_string(),
                                "sinceState": owned_state
                            }),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert!(response.method_responses[1].1["description"]
            .as_str()
            .unwrap()
            .contains("state does not match requested account"));
    }

    #[tokio::test]
    async fn mailbox_and_email_query_changes_replay_snapshot_differences() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::draft_mailbox()],
                emails: vec![FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall("Mailbox/query".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall("Email/query".to_string(), json!({}), "c2".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        let mailbox_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let email_query_state = initial_response.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/queryChanges".to_string(),
                            json!({"sinceQueryState": mailbox_query_state}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/queryChanges".to_string(),
                            json!({"sinceQueryState": email_query_state}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(response.method_responses[0].1["removed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(FakeStore::draft_mailbox().id.to_string())));
        assert_eq!(
            response.method_responses[0].1["added"][0]["id"],
            Value::String(FakeStore::inbox_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["added"][0]["id"],
            Value::String(FakeStore::inbox_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn email_changes_report_updates_for_existing_messages() {
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let prior_state = initial_response.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let mut updated_email = FakeStore::draft_email();
        updated_email.flagged = true;
        updated_email.preview = "Updated preview".to_string();
        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![updated_email],
            ..Default::default()
        });
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/changes".to_string(),
                        json!({"sinceState": prior_state}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["updated"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn paged_query_states_keep_full_mailbox_and_email_snapshots() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/query".to_string(),
                            json!({"position": 0, "limit": 1}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/query".to_string(),
                            json!({"position": 0, "limit": 1}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        let mailbox_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let email_query_state = initial_response.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox()],
                emails: vec![FakeStore::inbox_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/queryChanges".to_string(),
                            json!({"sinceQueryState": mailbox_query_state}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/queryChanges".to_string(),
                            json!({"sinceQueryState": email_query_state}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["removed"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["removed"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn email_query_changes_exact_limit_does_not_report_more_changes() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                emails: vec![FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/query".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let email_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                emails: vec![FakeStore::draft_email(), FakeStore::inbox_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/queryChanges".to_string(),
                        json!({"sinceQueryState": email_query_state, "maxChanges": 1}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["hasMoreChanges"],
            Value::Bool(false)
        );
        assert_eq!(
            response.method_responses[0].1["added"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn email_query_changes_reports_existing_message_reorders() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                emails: vec![FakeStore::draft_email(), FakeStore::inbox_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/query".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let email_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/queryChanges".to_string(),
                        json!({"sinceQueryState": email_query_state, "maxChanges": 4}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["hasMoreChanges"],
            Value::Bool(false)
        );
        assert_eq!(
            response.method_responses[0].1["removed"],
            json!([
                FakeStore::draft_email().id.to_string(),
                FakeStore::inbox_email().id.to_string()
            ])
        );
        assert_eq!(
            response.method_responses[0].1["added"],
            json!([
                {"id": FakeStore::inbox_email().id.to_string(), "index": 0},
                {"id": FakeStore::draft_email().id.to_string(), "index": 1}
            ])
        );
    }

    #[tokio::test]
    async fn mailbox_query_states_are_bound_to_the_requested_account() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall("Mailbox/query".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Mailbox/query".to_string(),
                            json!({"accountId": shared.account_id.to_string()}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_ne!(
            response.method_responses[0].1["queryState"],
            response.method_responses[1].1["queryState"]
        );
    }

    #[tokio::test]
    async fn mailbox_query_changes_reject_cross_account_query_state_replay() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/query".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let query_state = initial.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let replay = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/queryChanges".to_string(),
                        json!({
                            "accountId": shared.account_id.to_string(),
                            "sinceQueryState": query_state
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            replay.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert!(replay.method_responses[0].1["description"]
            .as_str()
            .unwrap()
            .contains("requested account"));
    }

    #[tokio::test]
    async fn mailbox_query_changes_report_mailbox_reorders() {
        let first_mailbox = JmapMailbox {
            id: Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap(),
            role: "".to_string(),
            name: "Alpha".to_string(),
            sort_order: 10,
            total_emails: 0,
            unread_emails: 0,
        };
        let second_mailbox = JmapMailbox {
            id: Uuid::parse_str("62626262-6262-6262-6262-626262626262").unwrap(),
            role: "".to_string(),
            name: "Bravo".to_string(),
            sort_order: 20,
            total_emails: 0,
            unread_emails: 0,
        };
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![first_mailbox.clone(), second_mailbox.clone()],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/query".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![
                JmapMailbox {
                    sort_order: 30,
                    ..first_mailbox.clone()
                },
                second_mailbox.clone(),
            ],
            ..Default::default()
        });
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/queryChanges".to_string(),
                        json!({"sinceQueryState": query_state}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let removed = response.method_responses[0].1["removed"]
            .as_array()
            .unwrap();
        let added = response.method_responses[0].1["added"].as_array().unwrap();
        assert!(removed
            .iter()
            .any(|value| value == &Value::String(first_mailbox.id.to_string())));
        assert!(removed
            .iter()
            .any(|value| value == &Value::String(second_mailbox.id.to_string())));
        assert!(added.iter().any(|value| {
            value["id"] == Value::String(second_mailbox.id.to_string())
                && value["index"] == Value::from(0)
        }));
        assert!(added.iter().any(|value| {
            value["id"] == Value::String(first_mailbox.id.to_string())
                && value["index"] == Value::from(1)
        }));
    }

    #[tokio::test]
    async fn identity_thread_and_submission_reads_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Identity/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Thread/get".to_string(),
                            json!({"ids": [FakeStore::draft_email().thread_id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "EmailSubmission/get".to_string(),
                            json!({"ids": ["11111111-2222-3333-4444-555555555555"]}),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["email"],
            Value::String("alice@example.test".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["emailIds"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
        assert_eq!(
            response.method_responses[2].1["list"][0]["deliveryStatus"],
            Value::String("queued".to_string())
        );
    }

    #[tokio::test]
    async fn identity_get_state_tracks_sender_identity_projection() {
        let account = FakeStore::account();
        let first_identity = FakeStore::sender_identity();
        let mut renamed_identity = first_identity.clone();
        renamed_identity.display_name = "Alice Delegated".to_string();

        let first_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            sender_identities: vec![first_identity],
            ..Default::default()
        });
        let renamed_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            sender_identities: vec![renamed_identity],
            ..Default::default()
        });

        let first = first_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let renamed = renamed_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let first_state = first.method_responses[0].1["state"].as_str().unwrap();
        let renamed_state = renamed.method_responses[0].1["state"].as_str().unwrap();
        let decoded = decode_state(first_state).unwrap();

        assert_eq!(decoded.kind, "Identity");
        assert_eq!(decoded.entries.len(), 1);
        assert_ne!(first_state, renamed_state);
    }

    #[tokio::test]
    async fn identity_changes_tracks_sender_identity_projection() {
        let account = FakeStore::account();
        let identity = FakeStore::sender_identity();
        let service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            sender_identities: vec![identity.clone()],
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/changes".to_string(),
                        json!({"sinceState": "0"}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].0, "Identity/changes");
        assert_eq!(
            response.method_responses[0].1["created"],
            json!([identity.id])
        );
        assert_eq!(
            decode_state(response.method_responses[0].1["newState"].as_str().unwrap())
                .unwrap()
                .kind,
            "Identity"
        );
    }

    #[tokio::test]
    async fn email_submission_get_state_tracks_submission_rows() {
        let queued = FakeStore::email_submission();
        let mut delivered = queued.clone();
        delivered.delivery_status = "delivered".to_string();
        let queued_service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            email_submissions: vec![queued.clone()],
            ..Default::default()
        });
        let delivered_service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            email_submissions: vec![delivered],
            ..Default::default()
        });

        let queued_response = queued_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/get".to_string(),
                        json!({"ids": [queued.id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let delivered_response = delivered_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/get".to_string(),
                        json!({"ids": [queued.id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let queued_state = queued_response.method_responses[0].1["state"]
            .as_str()
            .unwrap();
        let delivered_state = delivered_response.method_responses[0].1["state"]
            .as_str()
            .unwrap();
        let decoded = decode_state(queued_state).unwrap();

        assert_eq!(decoded.kind, "EmailSubmission");
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].id, queued.id.to_string());
        assert_ne!(queued_state, delivered_state);
    }

    #[tokio::test]
    async fn email_submission_changes_tracks_submission_rows() {
        let queued = FakeStore::email_submission();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            email_submissions: vec![queued.clone()],
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/changes".to_string(),
                        json!({"sinceState": "0"}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].0, "EmailSubmission/changes");
        assert_eq!(
            response.method_responses[0].1["created"],
            json!([queued.id.to_string()])
        );
        assert_eq!(
            decode_state(response.method_responses[0].1["newState"].as_str().unwrap())
                .unwrap()
                .kind,
            "EmailSubmission"
        );
    }

    #[tokio::test]
    async fn email_submission_query_filters_sorts_and_reports_query_changes() {
        let first = FakeStore::email_submission();
        let mut second = first.clone();
        second.id = Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap();
        second.email_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        second.thread_id = Uuid::parse_str("12121212-3434-5656-7878-909090909090").unwrap();
        second.send_at = "2026-04-18T10:02:00Z".to_string();
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            email_submissions: vec![first.clone()],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/query".to_string(),
                        json!({
                            "filter": {"undoStatus": "final"},
                            "sort": [{"property": "sentAt", "isAscending": true}],
                            "limit": 1
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        assert_eq!(
            initial_response.method_responses[0].0,
            "EmailSubmission/query"
        );
        assert_eq!(
            initial_response.method_responses[0].1["canCalculateChanges"],
            true
        );
        assert_eq!(
            initial_response.method_responses[0].1["ids"],
            json!([first.id.to_string()])
        );

        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            email_submissions: vec![first, second.clone()],
            ..Default::default()
        });
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/queryChanges".to_string(),
                        json!({
                            "sinceQueryState": query_state,
                            "filter": {"undoStatus": "final"},
                            "sort": [{"property": "sentAt", "isAscending": true}]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["added"],
            json!([{"id": second.id.to_string(), "index": 1}])
        );
        assert_eq!(response.method_responses[0].1["hasMoreChanges"], false);
    }

    #[tokio::test]
    async fn thread_query_returns_distinct_threads_for_filtered_emails() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/query".to_string(),
                        json!({"filter": {"inMailbox": FakeStore::inbox_mailbox().id.to_string()}}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"][0],
            Value::String(FakeStore::inbox_email().thread_id.to_string())
        );
        assert_eq!(
            response.method_responses[0].1["total"],
            Value::Number(1.into())
        );
        assert!(response.method_responses[0].1["queryState"].is_string());
        assert_eq!(
            response.method_responses[0].1["canCalculateChanges"],
            Value::Bool(true)
        );
    }

    #[tokio::test]
    async fn thread_query_state_keeps_full_snapshot_when_page_is_limited() {
        let mut second_thread_email = FakeStore::draft_email();
        second_thread_email.thread_id =
            Uuid::parse_str("12121212-3434-5656-7878-909090909090").unwrap();
        let service = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), second_thread_email],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/query".to_string(),
                        json!({"position": 0, "limit": 1}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let query_state = response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let decoded = decode_query_state(&query_state).unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(decoded.ids.len(), 2);
    }

    #[tokio::test]
    async fn thread_query_changes_reports_added_threads_from_full_snapshot() {
        let mut second_thread_email = FakeStore::draft_email();
        second_thread_email.thread_id =
            Uuid::parse_str("12121212-3434-5656-7878-909090909090").unwrap();
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![FakeStore::inbox_email()],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/query".to_string(),
                        json!({"position": 0, "limit": 1}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![FakeStore::inbox_email(), second_thread_email.clone()],
            ..Default::default()
        });
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/queryChanges".to_string(),
                        json!({
                            "sinceQueryState": query_state,
                            "maxChanges": 1
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].0, "Thread/queryChanges");
        assert_eq!(
            response.method_responses[0].1["added"][0]["id"],
            Value::String(second_thread_email.thread_id.to_string())
        );
        assert_eq!(response.method_responses[0].1["added"][0]["index"], 1);
        assert_eq!(
            response.method_responses[0].1["hasMoreChanges"],
            Value::Bool(false)
        );
    }

    #[tokio::test]
    async fn search_snippets_return_preview_for_requested_messages() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "SearchSnippet/get".to_string(),
                        json!({"emailIds": [FakeStore::draft_email().id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["preview"],
            Value::String("Draft preview".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_set_copy_import_and_quota_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 82,
            blob_bytes: b"From: Alice <alice@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Imported\r\n\r\nHello".to_vec(),
        });
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/set".to_string(),
                            json!({"create": {"m1": {"name": "Archive"}}}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({"fromAccountId": FakeStore::account().account_id.to_string(), "create": {"e1": {"emailId": FakeStore::draft_email().id.to_string(), "mailboxIds": {"99999999-9999-9999-9999-999999999999": true}}}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall("Quota/get".to_string(), json!({}), "c4".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"]["m1"]["id"],
            Value::String("99999999-9999-9999-9999-999999999999".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"]["e1"]["id"],
            Value::String("66666666-6666-6666-6666-666666666666".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["id"],
            Value::String("55555555-5555-5555-5555-555555555555".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["blobId"],
            Value::String("upload:77777777-7777-7777-7777-777777777777".to_string())
        );
        assert_eq!(
            response.method_responses[3].1["list"][0]["hardLimit"],
            Value::Number(100.into())
        );
    }

    #[tokio::test]
    async fn mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations() {
        let read_only = FakeStore::shared_mailbox_read_only_access(true, false);
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access(), read_only],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());
        let shared_account_id = FakeStore::shared_account().account_id.to_string();
        let draft_id = FakeStore::draft_email().id.to_string();
        let draft_mailbox_id = FakeStore::draft_mailbox().id.to_string();

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/set".to_string(),
                            json!({
                                "accountId": shared_account_id,
                                "create": {"m1": {"name": "Archive"}},
                                "update": {draft_mailbox_id: {"name": "Renamed"}},
                                "destroy": [FakeStore::draft_mailbox().id.to_string()]
                            }),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "fromAccountId": FakeStore::shared_account().account_id.to_string(),
                                "create": {
                                    "e1": {
                                        "emailId": draft_id,
                                        "mailboxIds": {FakeStore::draft_mailbox().id.to_string(): true}
                                    }
                                }
                            }),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "emails": {
                                    "i1": {
                                        "blobId": "77777777-7777-7777-7777-777777777777",
                                        "mailboxIds": {FakeStore::draft_mailbox().id.to_string(): true}
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["notCreated"]["m1"]["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[0].1["notUpdated"][FakeStore::draft_mailbox().id.to_string()]
                ["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[0].1["notDestroyed"]
                [FakeStore::draft_mailbox().id.to_string()]["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[1].1["notCreated"]["e1"]["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(
            response.method_responses[2].1["notCreated"]["i1"]["description"],
            "write access is not granted on this mailbox account"
        );
        assert_eq!(store.imported_emails.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn email_copy_and_import_reject_shared_drafts_without_sender_delegation() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "fromAccountId": FakeStore::shared_account().account_id.to_string(),
                                "create": {
                                    "e1": {
                                        "emailId": FakeStore::draft_email().id.to_string(),
                                        "mailboxIds": {FakeStore::draft_mailbox().id.to_string(): true}
                                    }
                                }
                            }),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({
                                "accountId": FakeStore::shared_account().account_id.to_string(),
                                "emails": {
                                    "i1": {
                                        "blobId": "77777777-7777-7777-7777-777777777777",
                                        "mailboxIds": {FakeStore::draft_mailbox().id.to_string(): true}
                                    }
                                }
                            }),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["notCreated"]["e1"]["description"],
            "sender delegation is required to write drafts in this mailbox account"
        );
        assert_eq!(
            response.method_responses[1].1["notCreated"]["i1"]["description"],
            "sender delegation is required to write drafts in this mailbox account"
        );
        assert_eq!(store.imported_emails.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn email_import_validates_and_preserves_multipart_attachments() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 321,
            blob_bytes: concat!(
                "From: Alice <alice@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Imported\r\n",
                "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
                "\r\n",
                "--b1\r\n",
                "Content-Type: multipart/alternative; boundary=\"b2\"\r\n",
                "\r\n",
                "--b2\r\n",
                "Content-Type: text/plain\r\n",
                "\r\n",
                "Hello plain\r\n",
                "--b2\r\n",
                "Content-Type: text/html\r\n",
                "\r\n",
                "<p>Hello html</p>\r\n",
                "--b2--\r\n",
                "--b1\r\n",
                "Content-Type: application/pdf\r\n",
                "Content-Disposition: attachment; filename=\"report.pdf\"\r\n",
                "\r\n",
                "%PDF-1.7\r\n",
                "--b1--\r\n"
            )
            .as_bytes()
            .to_vec(),
        });
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_sequence(vec![
                Ok(MagikaDetection {
                    label: "email".to_string(),
                    mime_type: "message/rfc822".to_string(),
                    description: "email".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["eml".to_string()],
                    score: Some(0.99),
                }),
                Ok(MagikaDetection {
                    label: "pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    description: "pdf".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["pdf".to_string()],
                    score: Some(0.99),
                }),
            ]),
        );

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/import".to_string(),
                        json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let imported = store.imported_emails.lock().unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].body_text, "Hello plain");
        assert_eq!(
            imported[0].body_html_sanitized.as_deref(),
            Some("<p>Hello html</p>")
        );
        assert_eq!(imported[0].attachments.len(), 1);
        assert_eq!(imported[0].attachments[0].file_name, "report.pdf");
        assert_eq!(imported[0].attachments[0].media_type, "application/pdf");
    }

    #[tokio::test]
    async fn email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "message/rfc822".to_string(),
                octet_size: 9,
                blob_bytes: b"mime-body".to_vec(),
            }])),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({"ids": [
                            FakeStore::inbox_email().id.to_string(),
                            FakeStore::draft_email().id.to_string()
                        ]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["blobId"],
            Value::String("upload:88888888-8888-8888-8888-888888888888".to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["blobId"],
            Value::String("draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "upload:88888888-8888-8888-8888-888888888888",
            )
            .await
            .unwrap();
        assert_eq!(blob.blob_bytes, b"mime-body".to_vec());

        let draft_blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc",
            )
            .await
            .unwrap();
        let draft_message = String::from_utf8(draft_blob.blob_bytes).unwrap();
        assert_eq!(draft_blob.media_type, "message/rfc822");
        assert!(draft_message.contains("Subject: Draft subject\r\n"));
        assert!(draft_message.contains("To: Bob <bob@example.test>\r\n"));
        assert!(draft_message.contains("Bcc: hidden@example.test\r\n"));
        assert!(draft_message.contains("\r\nDraft body\r\n"));
    }

    #[tokio::test]
    async fn message_blob_download_hides_bcc_for_delegated_shared_mailbox() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::shared_account().account_id.to_string(),
                "message:cccccccc-cccc-cccc-cccc-cccccccccccc",
            )
            .await
            .unwrap();
        let message = String::from_utf8(blob.blob_bytes).unwrap();

        assert!(message.contains("Subject: Draft subject\r\n"));
        assert!(!message.contains("Bcc:"));
        assert!(!message.contains("hidden@example.test"));
    }

    #[tokio::test]
    async fn blob_get_hides_bcc_for_delegated_shared_mailbox_message() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Blob/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "ids": ["message:cccccccc-cccc-cccc-cccc-cccccccccccc"],
                            "properties": ["data:asText", "digest:sha-256", "size"]
                        }),
                        "g1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let message = response.method_responses[0].1["list"][0]["data:asText"]
            .as_str()
            .unwrap();
        assert!(message.contains("Subject: Draft subject\r\n"));
        assert!(!message.contains("Bcc:"));
        assert!(!message.contains("hidden@example.test"));
        use base64::Engine as _;
        use sha2::Digest as _;
        let digest = base64::engine::general_purpose::STANDARD
            .encode(sha2::Sha256::digest(message.as_bytes()));
        assert_eq!(
            response.method_responses[0].1["list"][0]["digest:sha-256"],
            digest
        );
    }

    #[tokio::test]
    async fn blob_copy_copies_upload_and_message_blobs_through_canonical_blob_pipeline() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "message/rfc822".to_string(),
                octet_size: 9,
                blob_bytes: b"mime-body".to_vec(),
            }])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Blob/copy".to_string(),
                        json!({
                            "fromAccountId": FakeStore::account().account_id.to_string(),
                            "accountId": FakeStore::account().account_id.to_string(),
                            "blobIds": [
                                "upload:88888888-8888-8888-8888-888888888888",
                                "message:cccccccc-cccc-cccc-cccc-cccccccccccc",
                                "missing"
                            ]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let copied = &response.method_responses[0].1["copied"];
        assert!(copied["upload:88888888-8888-8888-8888-888888888888"]
            .as_str()
            .unwrap()
            .starts_with("upload:"));
        assert!(copied["message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
            .as_str()
            .unwrap()
            .starts_with("upload:"));
        assert_eq!(
            response.method_responses[0].1["notCopied"]["missing"]["description"],
            "blob not found"
        );
        let uploads = store.uploads.lock().unwrap();
        assert!(uploads.iter().any(|blob| blob.blob_bytes == b"mime-body"));
        assert!(uploads.iter().any(|blob| {
            String::from_utf8_lossy(&blob.blob_bytes).contains("Subject: Draft subject")
        }));
    }

    #[tokio::test]
    async fn blob_copy_to_shared_account_does_not_widen_owner_bcc() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Blob/copy".to_string(),
                        json!({
                            "fromAccountId": FakeStore::account().account_id.to_string(),
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "blobIds": ["message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(response.method_responses[0].1["copied"]
            ["message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
            .as_str()
            .unwrap()
            .starts_with("upload:"));
        let uploads = store.uploads.lock().unwrap();
        let copied = uploads
            .iter()
            .find(|blob| blob.account_id == FakeStore::shared_account().account_id)
            .expect("copied shared-account blob");
        let message = String::from_utf8(copied.blob_bytes.clone()).unwrap();
        assert!(message.contains("Subject: Draft subject\r\n"));
        assert!(!message.contains("Bcc:"));
        assert!(!message.contains("hidden@example.test"));
    }

    #[tokio::test]
    async fn blob_upload_get_and_copy_resolve_created_blob_references() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_sequence(vec![
                Ok(MagikaDetection {
                    label: "text".to_string(),
                    mime_type: "text/plain".to_string(),
                    description: "text".to_string(),
                    group: "text".to_string(),
                    extensions: vec!["txt".to_string()],
                    score: Some(0.99),
                }),
                Ok(MagikaDetection {
                    label: "text".to_string(),
                    mime_type: "text/plain".to_string(),
                    description: "text".to_string(),
                    group: "text".to_string(),
                    extensions: vec!["txt".to_string()],
                    score: Some(0.99),
                }),
            ]),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Blob/upload".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "create": {
                                    "b1": {
                                        "type": "text/plain",
                                        "data": [{"data:asText": "hello world"}]
                                    }
                                }
                            }),
                            "u1".to_string(),
                        ),
                        JmapMethodCall(
                            "Blob/upload".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "create": {
                                    "b2": {
                                        "type": "text/plain",
                                        "data": [{"blobId": "#b1", "offset": 6, "length": 5}]
                                    }
                                }
                            }),
                            "u2".to_string(),
                        ),
                        JmapMethodCall(
                            "Blob/get".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "ids": ["#b2"],
                                "properties": ["data:asText", "size"]
                            }),
                            "g1".to_string(),
                        ),
                        JmapMethodCall(
                            "Blob/copy".to_string(),
                            json!({
                                "fromAccountId": FakeStore::account().account_id.to_string(),
                                "accountId": FakeStore::account().account_id.to_string(),
                                "blobIds": ["#b2"]
                            }),
                            "c1".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(response.created_ids["b1"].starts_with("upload:"));
        assert!(response.created_ids["b2"].starts_with("upload:"));
        assert_eq!(response.method_responses[0].1["oldState"], Value::Null);
        assert_eq!(response.method_responses[0].1["updated"], Value::Null);
        assert_eq!(response.method_responses[0].1["destroyed"], Value::Null);
        assert_eq!(response.method_responses[0].1["notUpdated"], Value::Null);
        assert_eq!(response.method_responses[0].1["notDestroyed"], Value::Null);
        assert_eq!(
            response.method_responses[2].1["list"][0]["id"],
            response.created_ids["b2"]
        );
        assert_eq!(
            response.method_responses[2].1["list"][0]["data:asText"],
            "world"
        );
        assert_eq!(response.method_responses[2].1["list"][0]["size"], 5);
        assert!(response.method_responses[3].1["copied"]["#b2"]
            .as_str()
            .unwrap()
            .starts_with("upload:"));
        let uploads = store.uploads.lock().unwrap();
        assert!(uploads.iter().any(|blob| blob.blob_bytes == b"hello world"));
        assert!(uploads.iter().any(|blob| blob.blob_bytes == b"world"));
    }

    #[tokio::test]
    async fn blob_create_paths_reject_read_only_shared_accounts() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_read_only_access(false, false),
            ],
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "text/plain".to_string(),
                octet_size: 5,
                blob_bytes: b"hello".to_vec(),
            }])),
            ..Default::default()
        };
        let service =
            JmapService::new_with_validator(store, validator_ok("text/plain", "text", "txt", 0.99));

        let upload_response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Blob/upload".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "b1": {
                                    "type": "text/plain",
                                    "data": [{"data:asText": "blocked"}]
                                }
                            }
                        }),
                        "u1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            upload_response.method_responses[0].1["type"],
            "invalidArguments"
        );
        assert_eq!(
            upload_response.method_responses[0].1["description"],
            "accountId is read-only"
        );

        let copy_response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Blob/copy".to_string(),
                        json!({
                            "fromAccountId": FakeStore::account().account_id.to_string(),
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "blobIds": ["upload:99999999-9999-9999-9999-999999999999"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            copy_response.method_responses[0].1["type"],
            "invalidArguments"
        );
        assert_eq!(
            copy_response.method_responses[0].1["description"],
            "accountId is read-only"
        );

        let http_upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::shared_account().account_id.to_string(),
                "text/plain",
                b"blocked",
            )
            .await
            .unwrap_err();
        assert_eq!(http_upload.to_string(), "accountId is read-only");
    }

    #[tokio::test]
    async fn method_dispatch_requires_declared_capabilities() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Email/get".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "ids": ["cccccccc-cccc-cccc-cccc-cccccccccccc"]
                            }),
                            "e1".to_string(),
                        ),
                        JmapMethodCall(
                            "Blob/get".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "ids": ["message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
                            }),
                            "b1".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].1["type"], "unknownMethod");
        assert_eq!(
            response.method_responses[0].1["description"],
            "method capability is not requested"
        );
        assert_eq!(response.method_responses[1].1["type"], "unknownMethod");
        assert_eq!(
            response.method_responses[1].1["description"],
            "method capability is not requested"
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_BLOB_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Blob/copy".to_string(),
                        json!({
                            "fromAccountId": FakeStore::account().account_id.to_string(),
                            "accountId": FakeStore::account().account_id.to_string(),
                            "blobIds": ["message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].1["type"], "unknownMethod");
        assert_eq!(
            response.method_responses[0].1["description"],
            "method capability is not requested"
        );
    }

    #[tokio::test]
    async fn api_request_rejects_batches_beyond_advertised_call_limit() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });
        let method_calls = (0..=MAX_CALLS_IN_REQUEST)
            .map(|index| {
                JmapMethodCall("Mailbox/query".to_string(), json!({}), format!("c{index}"))
            })
            .collect();

        let error = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "JMAP request exceeds maxCallsInRequest");
    }

    #[tokio::test]
    async fn api_request_rejects_unsupported_declared_capabilities() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let error = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        "urn:ietf:params:jmap:unknown".to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/query".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "JMAP request declares unsupported capability: urn:ietf:params:jmap:unknown"
        );
    }

    #[tokio::test]
    async fn api_request_rejects_object_batches_beyond_advertised_limits() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });
        let ids = (0..=MAX_OBJECTS_IN_GET)
            .map(|index| format!("id-{index}"))
            .collect::<Vec<_>>();
        let mut create = serde_json::Map::new();
        for index in 0..=MAX_OBJECTS_IN_SET {
            create.insert(format!("client-{index}"), json!({}));
        }

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Email/get".to_string(),
                            json!({"ids": ids}),
                            "get".to_string(),
                        ),
                        JmapMethodCall(
                            "ContactCard/set".to_string(),
                            json!({"create": Value::Object(create)}),
                            "set".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].1["type"], "tooManyObjects");
        assert!(response.method_responses[0].1["description"]
            .as_str()
            .unwrap()
            .contains(&format!("limit is {MAX_OBJECTS_IN_GET}")));
        assert_eq!(response.method_responses[1].1["type"], "tooManyObjects");
        assert!(response.method_responses[1].1["description"]
            .as_str()
            .unwrap()
            .contains(&format!("limit is {MAX_OBJECTS_IN_SET}")));
    }

    #[test]
    fn api_request_concurrency_permits_match_advertised_limit() {
        let permits = (0..MAX_CONCURRENT_REQUESTS)
            .map(|_| {
                crate::service::try_acquire_api_request_permit()
                    .expect("advertised concurrency permit should be available")
            })
            .collect::<Vec<_>>();

        assert!(crate::service::try_acquire_api_request_permit().is_none());

        drop(permits);
        assert!(crate::service::try_acquire_api_request_permit().is_some());
    }

    #[test]
    fn upload_concurrency_permits_match_advertised_limit() {
        let permits = (0..MAX_CONCURRENT_UPLOAD)
            .map(|_| {
                crate::service::try_acquire_upload_request_permit()
                    .expect("advertised upload permit should be available")
            })
            .collect::<Vec<_>>();

        assert!(crate::service::try_acquire_upload_request_permit().is_none());

        drop(permits);
        assert!(crate::service::try_acquire_upload_request_permit().is_some());
    }

    #[tokio::test]
    async fn jmap_tester_style_big_three_batch_has_stable_json_shapes() {
        let account_id = FakeStore::account().account_id.to_string();
        let contact_id = FakeStore::contact().id.to_string();
        let event_id = FakeStore::event().id.to_string();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            contacts: Arc::new(Mutex::new(vec![FakeStore::contact()])),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        });
        let request: JmapApiRequest = serde_json::from_value(json!({
            "using": [
                JMAP_CORE_CAPABILITY,
                JMAP_MAIL_CAPABILITY,
                JMAP_CONTACTS_CAPABILITY,
                JMAP_CALENDARS_CAPABILITY
            ],
            "methodCalls": [
                ["Mailbox/query", {
                    "accountId": account_id,
                    "position": 0,
                    "limit": 50
                }, "mailbox-query"],
                ["Mailbox/get", {
                    "accountId": account_id,
                    "ids": null,
                    "properties": ["id", "name", "role", "myRights"]
                }, "mailbox-get"],
                ["ContactCard/query", {
                    "accountId": account_id,
                    "position": 0,
                    "limit": 50
                }, "contact-query"],
                ["ContactCard/get", {
                    "accountId": account_id,
                    "ids": [contact_id],
                    "properties": ["id", "name", "emails"]
                }, "contact-get"],
                ["CalendarEvent/query", {
                    "accountId": account_id,
                    "position": 0,
                    "limit": 50
                }, "event-query"],
                ["CalendarEvent/get", {
                    "accountId": account_id,
                    "ids": [event_id],
                    "properties": ["id", "title", "calendarIds", "participants"]
                }, "event-get"]
            ]
        }))
        .unwrap();

        let response = service
            .handle_api_request(Some("Bearer token"), request)
            .await
            .unwrap();

        assert!(response.created_ids.is_empty());
        assert_eq!(response.method_responses.len(), 6);
        assert_eq!(
            response
                .method_responses
                .iter()
                .map(|response| (response.0.as_str(), response.2.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("Mailbox/query", "mailbox-query"),
                ("Mailbox/get", "mailbox-get"),
                ("ContactCard/query", "contact-query"),
                ("ContactCard/get", "contact-get"),
                ("CalendarEvent/query", "event-query"),
                ("CalendarEvent/get", "event-get"),
            ]
        );

        let mailbox_query = &response.method_responses[0].1;
        assert_eq!(mailbox_query["accountId"], account_id);
        assert_eq!(mailbox_query["position"].as_u64(), Some(0));
        assert_eq!(mailbox_query["total"].as_u64(), Some(1));
        assert_eq!(mailbox_query["canCalculateChanges"], true);
        assert!(mailbox_query["ids"][0].as_str().is_some());

        let mailbox_get = &response.method_responses[1].1;
        assert_eq!(mailbox_get["list"].as_array().unwrap().len(), 1);
        assert!(mailbox_get["notFound"].as_array().unwrap().is_empty());
        assert!(mailbox_get["list"][0]["id"].as_str().is_some());
        assert!(mailbox_get["list"][0]["myRights"]["mayReadItems"]
            .as_bool()
            .unwrap());

        let contact_query = &response.method_responses[2].1;
        assert_eq!(contact_query["total"].as_u64(), Some(1));
        assert_eq!(contact_query["ids"][0], contact_id);

        let contact_get = &response.method_responses[3].1;
        assert_eq!(contact_get["list"][0]["id"], contact_id);
        assert_eq!(
            contact_get["list"][0]["emails"]["main"]["address"],
            "bob@example.test"
        );

        let event_query = &response.method_responses[4].1;
        assert_eq!(event_query["total"].as_u64(), Some(1));
        assert_eq!(event_query["ids"][0], event_id);

        let event_get = &response.method_responses[5].1;
        assert_eq!(event_get["list"][0]["id"], event_id);
        assert_eq!(event_get["list"][0]["title"], "Standup");
        assert!(event_get["list"][0]["participants"].as_object().is_some());
    }

    #[tokio::test]
    async fn blob_get_reports_encoding_and_range_edge_cases() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "application/octet-stream".to_string(),
                octet_size: 2,
                blob_bytes: vec![0xff, 0xfe],
            }])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Blob/get".to_string(),
                        json!({
                            "accountId": FakeStore::account().account_id.to_string(),
                            "ids": ["upload:99999999-9999-9999-9999-999999999999"],
                            "properties": ["data", "data:asText", "data:asBase64", "size"],
                            "offset": 0,
                            "length": 10
                        }),
                        "g1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let blob = &response.method_responses[0].1["list"][0];
        assert_eq!(blob["size"], 2);
        assert_eq!(blob["data:asText"], Value::Null);
        assert_eq!(blob["data:asBase64"], "//4=");
        assert_eq!(blob["isEncodingProblem"], true);
        assert_eq!(blob["isTruncated"], true);
    }

    #[tokio::test]
    async fn blob_get_returns_sha256_digest_for_selected_range() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "text/plain".to_string(),
                octet_size: 11,
                blob_bytes: b"hello world".to_vec(),
            }])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Blob/get".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "ids": ["upload:99999999-9999-9999-9999-999999999999"],
                                "properties": ["digest:sha", "digest:sha-256", "size"],
                                "offset": 6,
                                "length": 5
                            }),
                            "g1".to_string(),
                        ),
                        JmapMethodCall(
                            "Blob/get".to_string(),
                            json!({
                                "accountId": FakeStore::account().account_id.to_string(),
                                "ids": ["upload:99999999-9999-9999-9999-999999999999"],
                                "properties": ["digest:md5"]
                            }),
                            "g2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        let blob = &response.method_responses[0].1["list"][0];
        assert_eq!(blob["size"], 11);
        assert_eq!(blob["digest:sha"], "fCEUM/AgcVl3Qeb/Wo6jR4mrv0M=");
        assert_eq!(
            blob["digest:sha-256"],
            "SG6kYiTRu0+2gPNPfJrZao8k7Ii+c+qOWmxlJg6cuKc="
        );
        assert_eq!(response.method_responses[1].1["type"], "invalidArguments");
        assert_eq!(
            response.method_responses[1].1["description"],
            "digest:md5 is not supported"
        );
    }

    #[tokio::test]
    async fn blob_lookup_projects_visible_canonical_message_references() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_BLOB_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Blob/lookup".to_string(),
                        json!({
                            "accountId": FakeStore::account().account_id.to_string(),
                            "typeNames": ["Email", "Thread", "Mailbox"],
                            "ids": [
                                "draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc",
                                "missing"
                            ]
                        }),
                        "l1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let list = response.method_responses[0].1["list"].as_array().unwrap();
        assert_eq!(
            list[0]["matchedIds"]["Email"][0],
            "cccccccc-cccc-cccc-cccc-cccccccccccc"
        );
        assert_eq!(
            list[0]["matchedIds"]["Thread"][0],
            "dddddddd-dddd-dddd-dddd-dddddddddddd"
        );
        assert_eq!(
            list[0]["matchedIds"]["Mailbox"][0],
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        );
        assert!(list[1]["matchedIds"]["Email"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(list[1]["matchedIds"]["Thread"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(list[1]["matchedIds"]["Mailbox"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn blob_lookup_requires_referenced_type_capability() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_BLOB_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Blob/lookup".to_string(),
                        json!({
                            "accountId": FakeStore::account().account_id.to_string(),
                            "typeNames": ["Email"],
                            "ids": ["draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc"]
                        }),
                        "l1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].1["type"], "unknownDataType");
        assert_eq!(
            response.method_responses[0].1["description"],
            "mail capability is required for Blob/lookup mail references"
        );
    }

    #[tokio::test]
    async fn session_exposes_contacts_and_calendars_capabilities() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(
                Some("Bearer token"),
                Some("wss://mail.example.test/jmap/ws"),
                None,
            )
            .await
            .unwrap();

        assert!(session.capabilities.contains_key(JMAP_CONTACTS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_CALENDARS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_TASKS_CAPABILITY));
        assert!(session
            .capabilities
            .contains_key(JMAP_VACATION_RESPONSE_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_BLOB_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_WEBSOCKET_CAPABILITY));
        assert_eq!(
            session.accounts[&FakeStore::account().account_id.to_string()].account_capabilities
                [JMAP_BLOB_CAPABILITY]["maxDataSources"],
            MAX_BLOB_DATA_SOURCES
        );
        assert_eq!(
            session.accounts[&FakeStore::account().account_id.to_string()].account_capabilities
                [JMAP_BLOB_CAPABILITY]["supportedDigestAlgorithms"],
            json!(["sha", "sha-256"])
        );
        assert_eq!(
            session.primary_accounts[JMAP_CONTACTS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_CALENDARS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_TASKS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_VACATION_RESPONSE_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
    }

    #[tokio::test]
    async fn vacation_response_get_projects_canonical_active_sieve_vacation() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: Arc::new(Mutex::new(Some(
                r#"require ["vacation"];
                   vacation :subject "Out" :days 3 "Away until Monday";"#
                    .to_string(),
            ))),
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_VACATION_RESPONSE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "VacationResponse/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let object = &response.method_responses[0].1["list"][0];
        assert_eq!(object["id"], "singleton");
        assert_eq!(object["isEnabled"], true);
        assert_eq!(object["subject"], "Out");
        assert_eq!(object["textBody"], "Away until Monday");
        assert_eq!(object["htmlBody"], Value::Null);
    }

    #[tokio::test]
    async fn vacation_response_get_returns_disabled_without_active_vacation() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: Arc::new(Mutex::new(Some(
                r#"require ["fileinto"]; keep;"#.to_string(),
            ))),
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_VACATION_RESPONSE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "VacationResponse/get".to_string(),
                        json!({
                            "ids": ["singleton", "other"],
                            "properties": ["id", "isEnabled", "subject", "textBody"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let body = &response.method_responses[0].1;
        assert_eq!(body["list"][0]["isEnabled"], false);
        assert_eq!(body["list"][0]["subject"], "");
        assert_eq!(body["notFound"], json!(["other"]));
    }

    #[tokio::test]
    async fn vacation_response_set_writes_canonical_active_sieve_script() {
        let active_sieve_script = Arc::new(Mutex::new(None));
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: active_sieve_script.clone(),
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_VACATION_RESPONSE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "VacationResponse/set".to_string(),
                        json!({
                            "create": {
                                "v1": {
                                    "isEnabled": true,
                                    "subject": "Away",
                                    "textBody": "Back next week"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.created_ids["v1"], "singleton");
        assert_eq!(
            response.method_responses[0].1["created"]["v1"]["isEnabled"],
            true
        );
        let script = active_sieve_script.lock().unwrap().clone().unwrap();
        assert!(script.contains("vacation :subject \"Away\" :days 7 \"Back next week\";"));
    }

    #[tokio::test]
    async fn vacation_response_set_destroy_disables_active_sieve_script() {
        let active_sieve_script = Arc::new(Mutex::new(Some(
            r#"require ["vacation"]; vacation :subject "Out" "Away";"#.to_string(),
        )));
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: active_sieve_script.clone(),
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_VACATION_RESPONSE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "VacationResponse/set".to_string(),
                        json!({"destroy": ["singleton"]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["destroyed"],
            json!(["singleton"])
        );
        assert!(active_sieve_script.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn vacation_response_set_update_preserves_omitted_fields() {
        let active_sieve_script = Arc::new(Mutex::new(Some(
            r#"require ["vacation"];
               vacation :subject "Out" :days 7 "Away until Monday";"#
                .to_string(),
        )));
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: active_sieve_script.clone(),
            ..Default::default()
        });

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_VACATION_RESPONSE_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "VacationResponse/set".to_string(),
                        json!({
                            "update": {
                                "singleton": {
                                    "subject": "Back later"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let updated = &response.method_responses[0].1["updated"]["singleton"];
        assert_eq!(updated["isEnabled"], true);
        assert_eq!(updated["subject"], "Back later");
        assert_eq!(updated["textBody"], "Away until Monday");
        let script = active_sieve_script.lock().unwrap().clone().unwrap();
        assert!(script.contains("vacation :subject \"Back later\" :days 7 \"Away until Monday\";"));
    }

    #[tokio::test]
    async fn websocket_push_states_include_shared_mailbox_accounts() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                MailboxAccountAccess {
                    account_id: shared.account_id,
                    email: shared.email,
                    display_name: shared.display_name,
                    is_owned: false,
                    may_read: true,
                    may_write: true,
                    may_send_as: false,
                    may_send_on_behalf: false,
                },
            ],
            ..Default::default()
        });
        let states = service
            .current_push_states(
                FakeStore::account().account_id,
                &HashSet::from([
                    "Mailbox".to_string(),
                    "AddressBook".to_string(),
                    "Task".to_string(),
                ]),
            )
            .await
            .unwrap();

        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("Mailbox"));
        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("AddressBook"));
        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("Task"));
        assert!(states[&shared.account_id.to_string()].contains_key("Mailbox"));
        assert!(!states[&shared.account_id.to_string()].contains_key("AddressBook"));
        assert!(!states[&shared.account_id.to_string()].contains_key("Task"));
    }

    #[tokio::test]
    async fn websocket_push_states_include_submission_identity_mail_types() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            sender_identities: vec![SenderIdentity {
                id: format!("send-as:{}", shared.account_id),
                owner_account_id: shared.account_id,
                email: shared.email,
                display_name: shared.display_name,
                authorization_kind: "send-as".to_string(),
                sender_address: None,
                sender_display: None,
            }],
            ..Default::default()
        });
        let states = service
            .current_push_states(
                FakeStore::account().account_id,
                &HashSet::from([
                    "Identity".to_string(),
                    "EmailSubmission".to_string(),
                    "EmailDelivery".to_string(),
                    "AddressBook".to_string(),
                ]),
            )
            .await
            .unwrap();

        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("Identity"));
        assert!(
            states[&FakeStore::account().account_id.to_string()].contains_key("EmailSubmission")
        );
        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("EmailDelivery"));
        assert!(states[&shared.account_id.to_string()].contains_key("Identity"));
        assert!(states[&shared.account_id.to_string()].contains_key("EmailSubmission"));
        assert!(states[&shared.account_id.to_string()].contains_key("EmailDelivery"));
        assert!(!states[&shared.account_id.to_string()].contains_key("AddressBook"));
    }

    #[tokio::test]
    async fn websocket_push_enable_sends_full_state_for_missing_or_stale_push_state() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                MailboxAccountAccess {
                    account_id: shared.account_id,
                    email: shared.email,
                    display_name: shared.display_name,
                    is_owned: false,
                    may_read: true,
                    may_write: true,
                    may_send_as: false,
                    may_send_on_behalf: false,
                },
            ],
            ..Default::default()
        });
        let states = service
            .current_push_states(
                FakeStore::account().account_id,
                &HashSet::from(["Mailbox".to_string()]),
            )
            .await
            .unwrap();
        let push_state = encode_push_state(&states, None).unwrap();
        let missing = service
            .recover_push_enable_change(
                FakeStore::account().account_id,
                &HashSet::from(["Mailbox".to_string()]),
                None,
                None,
                &states,
            )
            .await
            .unwrap();
        let stale = service
            .recover_push_enable_change(
                FakeStore::account().account_id,
                &HashSet::from(["Mailbox".to_string()]),
                Some("stale"),
                None,
                &states,
            )
            .await
            .unwrap();
        let current = service
            .recover_push_enable_change(
                FakeStore::account().account_id,
                &HashSet::from(["Mailbox".to_string()]),
                Some(push_state.as_str()),
                None,
                &states,
            )
            .await
            .unwrap();

        assert_eq!(missing, Some(states.clone()));
        assert_eq!(stale, Some(states.clone()));
        assert_eq!(current, None);
    }

    #[tokio::test]
    async fn websocket_push_enable_refreshes_cursor_for_unchanged_states() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["Task".to_string()]);
        let service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            canonical_change_cursor: Some(11),
            ..Default::default()
        });
        let states = service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let previous_push_state = encode_push_state(&states, Some(10)).unwrap();

        let changed = service
            .recover_push_enable_change(
                account.account_id,
                &enabled_types,
                Some(previous_push_state.as_str()),
                Some(11),
                &states,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(changed, HashMap::new());
    }

    #[tokio::test]
    async fn websocket_reconnect_recovers_task_changes_from_canonical_journal() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["Task".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            canonical_change_cursor: Some(10),
            ..Default::default()
        });
        let previous_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let previous_push_state = encode_push_state(&previous_states, Some(10)).unwrap();

        let mut updated_task = FakeStore::task();
        updated_task.updated_at = "2026-04-22T08:00:00Z".to_string();
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [account.account_id]);
        change_set.set_journal_cursor(11);
        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![updated_task])),
            canonical_change_cursor: Some(11),
            canonical_change_replay: CanonicalChangeReplay {
                change_set,
                current_cursor: Some(11),
                truncated: false,
            },
            ..Default::default()
        });
        let current_states = updated_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();

        let changed = updated_service
            .recover_push_enable_change(
                account.account_id,
                &enabled_types,
                Some(previous_push_state.as_str()),
                Some(11),
                &current_states,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(changed.len(), 1);
        assert!(changed[&account.account_id.to_string()].contains_key("Task"));
    }

    #[tokio::test]
    async fn websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal() {
        let account = FakeStore::account();
        let shared = FakeStore::shared_account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            canonical_change_cursor: Some(10),
            ..Default::default()
        });
        let previous_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let previous_push_state = encode_push_state(&previous_states, Some(10)).unwrap();

        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [shared.account_id]);
        change_set.set_journal_cursor(11);
        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            canonical_change_cursor: Some(11),
            canonical_change_replay: CanonicalChangeReplay {
                change_set,
                current_cursor: Some(11),
                truncated: false,
            },
            ..Default::default()
        });
        let current_states = updated_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();

        let changed = updated_service
            .recover_push_enable_change(
                account.account_id,
                &enabled_types,
                Some(previous_push_state.as_str()),
                Some(11),
                &current_states,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(
            changed[&shared.account_id.to_string()]["Mailbox"],
            current_states[&shared.account_id.to_string()]["Mailbox"]
        );
    }

    #[tokio::test]
    async fn websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["Task".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            canonical_change_cursor: Some(10),
            ..Default::default()
        });
        let previous_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let previous_push_state = encode_push_state(&previous_states, Some(10)).unwrap();

        let mut updated_task = FakeStore::task();
        updated_task.updated_at = "2026-04-22T09:00:00Z".to_string();
        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![updated_task])),
            canonical_change_cursor: Some(20),
            canonical_change_replay: CanonicalChangeReplay {
                change_set: CanonicalPushChangeSet::default(),
                current_cursor: Some(20),
                truncated: true,
            },
            ..Default::default()
        });
        let current_states = updated_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();

        let changed = updated_service
            .recover_push_enable_change(
                account.account_id,
                &enabled_types,
                Some(previous_push_state.as_str()),
                Some(20),
                &current_states,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(changed, current_states);
    }

    #[tokio::test]
    async fn scoped_push_change_is_stable_for_noop_mail_notifications() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            ..Default::default()
        });
        let last_type_states = service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states.clone());
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [account.account_id]);

        let (changed, current_type_states) = service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(changed.is_empty());
        assert_eq!(current_type_states, last_type_states);
    }

    #[tokio::test]
    async fn scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes() {
        let account = FakeStore::account();
        let shared = FakeStore::shared_account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [shared.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(!current_type_states.contains_key(&shared.account_id.to_string()));
        assert_eq!(
            changed[&account.account_id.to_string()]["Mailbox"],
            current_type_states[&account.account_id.to_string()]["Mailbox"]
        );
    }

    #[tokio::test]
    async fn scoped_push_change_reports_delegated_mailbox_right_changes() {
        let account = FakeStore::account();
        let shared = FakeStore::shared_account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [shared.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert_eq!(
            changed[&shared.account_id.to_string()]["Mailbox"],
            current_type_states[&shared.account_id.to_string()]["Mailbox"]
        );
    }

    #[tokio::test]
    async fn scoped_push_change_reports_delegated_identity_right_changes() {
        let account = FakeStore::account();
        let shared = FakeStore::shared_account();
        let enabled_types = HashSet::from(["Identity".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, false),
            ],
            sender_identities: vec![SenderIdentity {
                id: format!("send-as:{}", shared.account_id),
                owner_account_id: shared.account_id,
                email: shared.email.clone(),
                display_name: shared.display_name.clone(),
                authorization_kind: "send-as".to_string(),
                sender_address: None,
                sender_display: None,
            }],
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [shared.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        let shared_identity_state = &changed[&shared.account_id.to_string()]["Identity"];
        assert_eq!(
            shared_identity_state,
            &current_type_states[&shared.account_id.to_string()]["Identity"]
        );
        assert!(decode_state(shared_identity_state)
            .unwrap()
            .entries
            .is_empty());
    }

    #[tokio::test]
    async fn scoped_push_change_reports_email_delivery_for_new_messages_only_state() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["EmailDelivery".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            emails: vec![FakeStore::inbox_email()],
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();

        let mut changed_flags_email = FakeStore::inbox_email();
        changed_flags_email.flagged = true;
        changed_flags_email.preview = "Changed preview".to_string();
        let changed_flags_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            emails: vec![changed_flags_email],
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [account.account_id]);

        let (unchanged, unchanged_type_states) = changed_flags_service
            .compute_push_changes(
                account.account_id,
                &push_subscription(enabled_types.clone(), last_type_states.clone()),
                &change_set,
            )
            .await
            .unwrap();

        assert!(unchanged.is_empty());
        assert_eq!(unchanged_type_states, last_type_states);

        let mut new_email = FakeStore::draft_email();
        new_email.id = Uuid::parse_str("23232323-2323-2323-2323-232323232323").unwrap();
        new_email.received_at = "2026-04-20T11:00:00Z".to_string();
        let delivered_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            emails: vec![FakeStore::inbox_email(), new_email],
            ..Default::default()
        });

        let (changed, current_type_states) = delivered_service
            .compute_push_changes(
                account.account_id,
                &push_subscription(enabled_types, last_type_states),
                &change_set,
            )
            .await
            .unwrap();

        assert_eq!(
            changed[&account.account_id.to_string()]["EmailDelivery"],
            current_type_states[&account.account_id.to_string()]["EmailDelivery"]
        );
    }

    #[tokio::test]
    async fn scoped_push_change_limits_recompute_to_requested_categories() {
        let account = FakeStore::account();
        let mut updated_task = FakeStore::task();
        updated_task.updated_at = "2026-04-21T08:00:00Z".to_string();
        let enabled_types = HashSet::from(["Mailbox".to_string(), "Task".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            tasks: Arc::new(Mutex::new(vec![updated_task])),
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [account.account_id]);

        let (changed, _) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(changed[&account.account_id.to_string()].contains_key("Task"));
        assert!(!changed[&account.account_id.to_string()].contains_key("Mailbox"));
    }

    #[tokio::test]
    async fn shared_task_push_change_wakes_grantee_principal() {
        let account = FakeStore::account();
        let shared_owner = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("94949494-9494-9494-9494-949494949494").unwrap(),
            name: "Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: shared_owner.account_id,
            owner_email: shared_owner.email.clone(),
            owner_display_name: shared_owner.display_name.clone(),
            is_owned: false,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let shared_task_id = Uuid::parse_str("95959595-9595-9595-9595-959595959595").unwrap();
        let initial_shared_task = ClientTask {
            id: shared_task_id,
            owner_account_id: shared_task_list.owner_account_id,
            owner_email: shared_task_list.owner_email.clone(),
            owner_display_name: shared_task_list.owner_display_name.clone(),
            is_owned: false,
            rights: shared_task_list.rights.clone(),
            task_list_id: shared_task_list.id,
            task_list_sort_order: shared_task_list.sort_order,
            title: "Shared rollout".to_string(),
            description: "Visible through canonical sharing".to_string(),
            status: "in-progress".to_string(),
            due_at: None,
            completed_at: None,
            sort_order: 1,
            updated_at: "2026-04-20T16:20:00Z".to_string(),
        };
        let mut updated_shared_task = initial_shared_task.clone();
        updated_shared_task.updated_at = "2026-04-21T09:00:00Z".to_string();
        updated_shared_task.status = "completed".to_string();
        updated_shared_task.completed_at = Some("2026-04-21T09:00:00Z".to_string());

        let enabled_types = HashSet::from(["Task".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), initial_shared_task])),
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list,
            ])),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), updated_shared_task])),
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [shared_owner.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert_eq!(
            changed[&account.account_id.to_string()]["Task"],
            current_type_states[&account.account_id.to_string()]["Task"]
        );
    }

    #[tokio::test]
    async fn shared_task_list_rights_push_change_wakes_grantee_principal() {
        let account = FakeStore::account();
        let shared_owner = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("96969696-9696-9696-9696-969696969696").unwrap(),
            name: "Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: shared_owner.account_id,
            owner_email: shared_owner.email.clone(),
            owner_display_name: shared_owner.display_name.clone(),
            is_owned: false,
            rights: FakeStore::read_only_rights(),
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let enabled_types = HashSet::from(["TaskList".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let mut writable_shared_task_list = shared_task_list;
        writable_shared_task_list.rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                writable_shared_task_list,
            ])),
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [shared_owner.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert_eq!(
            changed[&account.account_id.to_string()]["TaskList"],
            current_type_states[&account.account_id.to_string()]["TaskList"]
        );
    }

    #[tokio::test]
    async fn deleted_shared_task_list_push_change_wakes_former_grantee_principal() {
        let account = FakeStore::account();
        let shared_owner = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("97979797-9797-9797-9797-979797979797").unwrap(),
            name: "Retired Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: shared_owner.account_id,
            owner_email: shared_owner.email.clone(),
            owner_display_name: shared_owner.display_name.clone(),
            is_owned: false,
            rights: FakeStore::read_only_rights(),
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let enabled_types = HashSet::from(["TaskList".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list,
            ])),
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            task_lists: Arc::new(Mutex::new(vec![FakeStore::default_task_list()])),
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [shared_owner.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert_eq!(
            changed[&account.account_id.to_string()]["TaskList"],
            current_type_states[&account.account_id.to_string()]["TaskList"]
        );
    }

    #[tokio::test]
    async fn contacts_methods_use_canonical_contact_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![FakeStore::contact()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("AddressBook/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "ContactCard/get".to_string(),
                            json!({"ids": [FakeStore::contact().id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "AddressBook/query".to_string(),
                            json!({}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall(
                            "ContactCard/query".to_string(),
                            json!({}),
                            "c4".to_string(),
                        ),
                        JmapMethodCall(
                            "ContactCard/set".to_string(),
                            json!({
                                "create": {
                                    "new1": {
                                        "name": {"full": "Carol Example"},
                                        "emails": {"main": {"address": "carol@example.test"}},
                                        "phones": {"main": {"number": "+339999"}},
                                        "organizations": {"main": {"name": "Ops"}},
                                        "titles": {"main": {"name": "Manager"}},
                                        "notes": {"main": {"note": "Priority"}},
                                        "addressBookIds": {"default": true}
                                    }
                                }
                            }),
                            "c5".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["name"]["full"],
            Value::String("Bob Example".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["canCalculateChanges"],
            Value::Bool(true)
        );
        assert_eq!(
            response.method_responses[3].1["canCalculateChanges"],
            Value::Bool(true)
        );
        assert!(response.created_ids.contains_key("new1"));
        let contacts = store.contacts.lock().unwrap();
        assert_eq!(contacts.len(), 2);
        assert!(contacts
            .iter()
            .any(|contact| contact.email == "carol@example.test"));
    }

    #[tokio::test]
    async fn calendar_methods_use_canonical_event_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Calendar/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall("Calendar/query".to_string(), json!({}), "c2".to_string()),
                        JmapMethodCall(
                            "CalendarEvent/query".to_string(),
                            json!({"filter": {"inCalendar": "default"}}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall(
                            "CalendarEvent/set".to_string(),
                            json!({
                                "create": {
                                    "ev1": {
                                        "@type": "Event",
                                        "title": "Planning",
                                        "start": "2026-04-21T11:00:00",
                                        "duration": "PT0S",
                                        "locations": {"main": {"name": "Room B"}},
                                        "participants": {
                                            "owner": {
                                                "name": "Alice",
                                                "email": "alice@example.test",
                                                "roles": {"owner": true}
                                            },
                                            "p1": {
                                                "name": "Bob",
                                                "email": "bob@example.test",
                                                "roles": {"attendee": true},
                                                "participationStatus": "accepted",
                                                "expectReply": true
                                            }
                                        },
                                        "description": "Weekly planning",
                                        "calendarIds": {"default": true}
                                    }
                                }
                            }),
                            "c4".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["canCalculateChanges"],
            Value::Bool(true)
        );
        assert_eq!(
            response.method_responses[2].1["ids"][0],
            Value::String(FakeStore::event().id.to_string())
        );
        assert_eq!(
            response.method_responses[2].1["canCalculateChanges"],
            Value::Bool(true)
        );
        let events = store.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event.title == "Planning"));
        let created = events
            .iter()
            .find(|event| event.title == "Planning")
            .unwrap();
        assert_eq!(created.attendees, "Bob");
        assert!(created.attendees_json.contains("\"organizer\""));
        assert!(created.attendees_json.contains("\"partstat\":\"accepted\""));
        assert!(created.attendees_json.contains("\"rsvp\":true"));
    }

    #[tokio::test]
    async fn contact_and_calendar_query_changes_report_reorders() {
        let contact_id = FakeStore::contact().id;
        let later_contact_id = Uuid::parse_str("13131313-1313-1313-1313-131313131313").unwrap();
        let event_id = FakeStore::event().id;
        let later_event_id = Uuid::parse_str("35353535-3535-3535-3535-353535353535").unwrap();
        let mut later_event = FakeStore::event();
        later_event.id = later_event_id;
        later_event.time = "11:00".to_string();
        later_event.title = "Later planning".to_string();

        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![
                FakeStore::contact(),
                ClientContact {
                    id: later_contact_id,
                    name: "Zoe Example".to_string(),
                    role: String::new(),
                    email: "zoe@example.test".to_string(),
                    phone: String::new(),
                    team: String::new(),
                    notes: String::new(),
                },
            ])),
            events: Arc::new(Mutex::new(vec![FakeStore::event(), later_event])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "ContactCard/query".to_string(),
                            json!({}),
                            "cq1".to_string(),
                        ),
                        JmapMethodCall(
                            "CalendarEvent/query".to_string(),
                            json!({}),
                            "eq1".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();
        let contact_query_state = initial.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let event_query_state = initial.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        {
            let mut contacts = store.contacts.lock().unwrap();
            let contact = contacts
                .iter_mut()
                .find(|contact| contact.id == contact_id)
                .unwrap();
            contact.name = "Zzz Example".to_string();
        }
        {
            let mut events = store.events.lock().unwrap();
            let event = events
                .iter_mut()
                .find(|event| event.id == event_id)
                .unwrap();
            event.time = "12:00".to_string();
        }

        let changes = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "ContactCard/queryChanges".to_string(),
                            json!({"sinceQueryState": contact_query_state}),
                            "cq2".to_string(),
                        ),
                        JmapMethodCall(
                            "CalendarEvent/queryChanges".to_string(),
                            json!({"sinceQueryState": event_query_state}),
                            "eq2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(changes.method_responses[0].1["removed"]
            .as_array()
            .unwrap()
            .contains(&json!(contact_id.to_string())));
        assert!(changes.method_responses[0].1["added"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["id"] == contact_id.to_string() && entry["index"] == 1));
        assert!(changes.method_responses[1].1["removed"]
            .as_array()
            .unwrap()
            .contains(&json!(event_id.to_string())));
        assert!(changes.method_responses[1].1["added"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["id"] == event_id.to_string() && entry["index"] == 1));
    }

    #[tokio::test]
    async fn address_book_and_calendar_query_changes_report_collection_reorders() {
        let mut shared_address_book = FakeStore::contact_collection();
        shared_address_book.id = "shared-contacts".to_string();
        shared_address_book.display_name = "Shared Contacts".to_string();
        shared_address_book.is_owned = false;
        shared_address_book.rights = FakeStore::read_only_rights();
        let mut shared_calendar = FakeStore::calendar_collection();
        shared_calendar.id = "shared-calendar".to_string();
        shared_calendar.display_name = "Shared Calendar".to_string();
        shared_calendar.is_owned = false;
        shared_calendar.rights = FakeStore::read_only_rights();

        let contact_collections = Arc::new(Mutex::new(vec![
            FakeStore::contact_collection(),
            shared_address_book.clone(),
        ]));
        let calendar_collections = Arc::new(Mutex::new(vec![
            FakeStore::calendar_collection(),
            shared_calendar.clone(),
        ]));
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            contact_collections: contact_collections.clone(),
            calendar_collections: calendar_collections.clone(),
            ..Default::default()
        });

        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "AddressBook/query".to_string(),
                            json!({}),
                            "ab1".to_string(),
                        ),
                        JmapMethodCall("Calendar/query".to_string(), json!({}), "cal1".to_string()),
                    ],
                },
            )
            .await
            .unwrap();
        let address_book_query_state = initial.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let calendar_query_state = initial.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        contact_collections.lock().unwrap().reverse();
        calendar_collections.lock().unwrap().reverse();

        let changes = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "AddressBook/queryChanges".to_string(),
                            json!({"sinceQueryState": address_book_query_state}),
                            "ab2".to_string(),
                        ),
                        JmapMethodCall(
                            "Calendar/queryChanges".to_string(),
                            json!({"sinceQueryState": calendar_query_state}),
                            "cal2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            changes.method_responses[0].1["removed"],
            json!(["default", "shared-contacts"])
        );
        assert_eq!(
            changes.method_responses[0].1["added"],
            json!([
                {"id": "shared-contacts", "index": 0},
                {"id": "default", "index": 1}
            ])
        );
        assert_eq!(
            changes.method_responses[1].1["removed"],
            json!(["default", "shared-calendar"])
        );
        assert_eq!(
            changes.method_responses[1].1["added"],
            json!([
                {"id": "shared-calendar", "index": 0},
                {"id": "default", "index": 1}
            ])
        );
    }

    #[tokio::test]
    async fn calendar_event_get_exposes_owner_and_participation_status() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "CalendarEvent/get".to_string(),
                        json!({
                            "ids": [FakeStore::event().id.to_string()],
                            "properties": ["id", "participants"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let event = &response.method_responses[0].1["list"][0];
        assert_eq!(
            event["participants"]["owner"]["email"],
            "alice@example.test"
        );
        assert_eq!(event["participants"]["owner"]["roles"]["owner"], true);
        assert_eq!(event["participants"]["p1"]["email"], "bob@example.test");
        assert_eq!(
            event["participants"]["p1"]["participationStatus"],
            "tentative"
        );
        assert_eq!(event["participants"]["p1"]["expectReply"], true);
    }

    #[tokio::test]
    async fn task_methods_use_canonical_task_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_TASKS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("TaskList/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Task/get".to_string(),
                            json!({"ids": [FakeStore::task().id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Task/set".to_string(),
                            json!({
                                "create": {
                                    "t1": {
                                        "@type": "Task",
                                        "title": "Follow up",
                                        "description": "Send customer recap",
                                        "status": "in-progress",
                                        "due": "2026-04-22T08:30:00Z",
                                        "sortOrder": 20,
                                        "taskListId": FakeStore::default_task_list().id.to_string()
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String(FakeStore::default_task_list().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["status"],
            Value::String("needs-action".to_string())
        );
        assert!(response.created_ids.contains_key("t1"));
        let tasks = store.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().any(|task| task.title == "Follow up"));
    }

    #[tokio::test]
    async fn task_list_get_projects_shared_task_list_rights() {
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("90909090-9090-9090-9090-909090909090").unwrap(),
            name: "Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: FakeStore::shared_account().account_id,
            owner_email: FakeStore::shared_account().email,
            owner_display_name: FakeStore::shared_account().display_name,
            is_owned: false,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "TaskList/get".to_string(),
                        json!({
                            "ids": [shared_task_list.id.to_string()],
                            "properties": ["id", "myRights"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let rights = &response.method_responses[0].1["list"][0]["myRights"];
        assert_eq!(rights["mayRead"], true);
        assert_eq!(rights["mayAddItems"], true);
        assert_eq!(rights["mayModifyItems"], true);
        assert_eq!(rights["mayRemoveItems"], false);
        assert_eq!(rights["mayRename"], false);
        assert_eq!(rights["mayDelete"], false);
        assert_eq!(rights["mayAdmin"], false);
    }

    #[tokio::test]
    async fn task_list_changes_tracks_shared_rights_updates() {
        let shared_account = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("90909090-9090-9090-9090-909090909090").unwrap(),
            name: "Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: shared_account.account_id,
            owner_email: shared_account.email,
            owner_display_name: shared_account.display_name,
            is_owned: false,
            rights: FakeStore::read_only_rights(),
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let initial_service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            ..Default::default()
        });

        let initial = initial_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "TaskList/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let state = initial.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let mut writable_shared_task_list = shared_task_list.clone();
        writable_shared_task_list.rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let updated_service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                writable_shared_task_list,
            ])),
            ..Default::default()
        });

        let changes = updated_service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "TaskList/changes".to_string(),
                        json!({"sinceState": state}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            changes.method_responses[0].1["updated"],
            json!([shared_task_list.id.to_string()])
        );
    }

    #[tokio::test]
    async fn task_query_includes_shared_accessible_tasks() {
        let shared_account = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("91919191-9191-9191-9191-919191919191").unwrap(),
            name: "Shared Ops".to_string(),
            role: None,
            sort_order: 40,
            owner_account_id: shared_account.account_id,
            owner_email: shared_account.email,
            owner_display_name: shared_account.display_name,
            is_owned: false,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            updated_at: "2026-04-20T16:10:00Z".to_string(),
        };
        let shared_task = ClientTask {
            id: Uuid::parse_str("92929292-9292-9292-9292-929292929292").unwrap(),
            owner_account_id: shared_task_list.owner_account_id,
            owner_email: "shared@example.test".to_string(),
            owner_display_name: "Shared Mailbox".to_string(),
            is_owned: false,
            rights: shared_task_list.rights.clone(),
            task_list_id: shared_task_list.id,
            task_list_sort_order: shared_task_list.sort_order,
            title: "Shared rollout".to_string(),
            description: "Visible through canonical sharing".to_string(),
            status: "in-progress".to_string(),
            due_at: None,
            completed_at: None,
            sort_order: 1,
            updated_at: "2026-04-20T16:20:00Z".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), shared_task.clone()])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Task/query".to_string(),
                        json!({
                            "filter": {"inTaskList": shared_task_list.id.to_string()}
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"],
            json!([shared_task.id.to_string()])
        );
    }

    #[tokio::test]
    async fn task_set_rejects_writes_to_read_only_shared_task_list() {
        let shared_account = FakeStore::shared_account();
        let shared_task_list = ClientTaskList {
            id: Uuid::parse_str("93939393-9393-9393-9393-939393939393").unwrap(),
            name: "Read Only".to_string(),
            role: None,
            sort_order: 50,
            owner_account_id: shared_account.account_id,
            owner_email: shared_account.email,
            owner_display_name: shared_account.display_name,
            is_owned: false,
            rights: FakeStore::read_only_rights(),
            updated_at: "2026-04-20T16:30:00Z".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                shared_task_list.clone(),
            ])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Task/set".to_string(),
                        json!({
                            "create": {
                                "t1": {
                                    "@type": "Task",
                                    "title": "Blocked",
                                    "taskListId": shared_task_list.id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["notCreated"]["t1"]["description"],
            "write access is not granted on this task list"
        );
    }

    #[tokio::test]
    async fn task_query_changes_tracks_sort_order_and_updates() {
        let mut second_task = FakeStore::task();
        second_task.id = Uuid::parse_str("67676767-6767-6767-6767-676767676767").unwrap();
        second_task.title = "Review notes".to_string();
        second_task.sort_order = 20;
        second_task.updated_at = "2026-04-20T15:10:00Z".to_string();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), second_task.clone()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Task/query".to_string(),
                            json!({"sort": [{"property": "sortOrder", "isAscending": true}]}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall("Task/get".to_string(), json!({}), "c2".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        let query_state = initial.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let task_state = initial.method_responses[1].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Task/set".to_string(),
                        json!({
                            "update": {
                                second_task.id.to_string(): {
                                    "title": "Review notes",
                                    "description": "Review architecture notes",
                                    "status": "completed",
                                    "completed": "2026-04-20T16:00:00Z",
                                    "sortOrder": 5,
                                    "taskListId": FakeStore::default_task_list().id.to_string()
                                }
                            }
                        }),
                        "c2".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let changes = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Task/queryChanges".to_string(),
                            json!({
                                "sinceQueryState": query_state,
                                "sort": [{"property": "sortOrder", "isAscending": true}]
                            }),
                            "c3".to_string(),
                        ),
                        JmapMethodCall(
                            "Task/changes".to_string(),
                            json!({
                                "sinceState": task_state
                            }),
                            "c4".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(changes.method_responses[0].1["removed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(second_task.id.to_string())));
        assert!(changes.method_responses[0].1["added"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["id"] == Value::String(second_task.id.to_string())));
        assert!(changes.method_responses[1].1["updated"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(second_task.id.to_string())));
    }

    #[tokio::test]
    async fn task_list_set_creates_updates_and_destroys_custom_lists() {
        let custom_task_list = ClientTaskList {
            id: Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap(),
            name: "Operations".to_string(),
            role: None,
            sort_order: 20,
            owner_account_id: FakeStore::account().account_id,
            owner_email: FakeStore::account().email,
            owner_display_name: FakeStore::account().display_name,
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            updated_at: "2026-04-20T15:10:00Z".to_string(),
        };
        let deletable_task_list = ClientTaskList {
            id: Uuid::parse_str("21212121-2121-2121-2121-212121212121").unwrap(),
            name: "Archive".to_string(),
            role: None,
            sort_order: 30,
            owner_account_id: FakeStore::account().account_id,
            owner_email: FakeStore::account().email,
            owner_display_name: FakeStore::account().display_name,
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            updated_at: "2026-04-20T15:10:00Z".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                custom_task_list.clone(),
                deletable_task_list.clone(),
            ])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "TaskList/set".to_string(),
                        json!({
                            "create": {
                                "newList": {
                                    "name": "Roadmap",
                                    "sortOrder": 30
                                }
                            },
                            "update": {
                                custom_task_list.id.to_string(): {
                                    "name": "Ops",
                                    "sortOrder": 5
                                }
                            },
                            "destroy": [deletable_task_list.id.to_string()]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(response.method_responses[0].1["created"]["newList"]["id"].is_string());
        assert_eq!(
            response.method_responses[0].1["updated"][&custom_task_list.id.to_string()]["name"],
            "Ops"
        );
        assert!(response.method_responses[0].1["destroyed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(deletable_task_list.id.to_string())));
    }

    #[tokio::test]
    async fn task_query_filters_and_reorders_across_task_lists() {
        let custom_task_list = ClientTaskList {
            id: Uuid::parse_str("30303030-3030-3030-3030-303030303030").unwrap(),
            name: "Ops".to_string(),
            role: None,
            sort_order: 50,
            owner_account_id: FakeStore::account().account_id,
            owner_email: FakeStore::account().email,
            owner_display_name: FakeStore::account().display_name,
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            updated_at: "2026-04-20T15:00:00Z".to_string(),
        };
        let mut custom_task = FakeStore::task();
        custom_task.id = Uuid::parse_str("40404040-4040-4040-4040-404040404040").unwrap();
        custom_task.task_list_id = custom_task_list.id;
        custom_task.task_list_sort_order = custom_task_list.sort_order;
        custom_task.sort_order = 1;
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_lists: Arc::new(Mutex::new(vec![
                FakeStore::default_task_list(),
                custom_task_list.clone(),
            ])),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), custom_task.clone()])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Task/query".to_string(),
                        json!({
                            "filter": {"inTaskList": custom_task_list.id.to_string()},
                            "sort": [{"property": "sortOrder", "isAscending": true}]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"],
            json!([custom_task.id.to_string()])
        );
    }

    #[tokio::test]
    async fn upload_and_download_use_authenticated_account() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();
        assert_eq!(
            upload["blobId"],
            Value::String("77777777-7777-7777-7777-777777777777".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "77777777-7777-7777-7777-777777777777",
            )
            .await
            .unwrap();
        assert_eq!(blob.media_type, "message/rfc822");
        assert_eq!(blob.blob_bytes, b"Subject: Hello\r\n\r\nBody".to_vec());
    }

    #[tokio::test]
    async fn upload_rejects_bodies_larger_than_session_limit() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );
        let oversized = vec![b'x'; MAX_SIZE_UPLOAD as usize + 1];

        let result = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                &oversized,
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "JMAP upload exceeds maxSizeUpload"
        );
    }

    #[tokio::test]
    async fn upload_accepts_validated_matching_blob() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();

        assert_eq!(upload["type"], Value::String("message/rfc822".to_string()));
    }

    #[tokio::test]
    async fn upload_rejects_declared_mime_mismatch() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("application/pdf", "pdf", "pdf", 0.99),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"%PDF-1.7",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_rejects_unknown_type() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            Validator::new(
                FakeDetector {
                    results: Arc::new(Mutex::new(vec![Ok(MagikaDetection {
                        label: "unknown_binary".to_string(),
                        mime_type: "application/octet-stream".to_string(),
                        description: "unknown".to_string(),
                        group: "unknown".to_string(),
                        extensions: Vec::new(),
                        score: Some(0.99),
                    })])),
                },
                0.80,
            ),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "application/octet-stream",
                b"\x00\x01\x02",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_surfaces_magika_failure_mode() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service =
            JmapService::new_with_validator(store, validator_error("Magika command failed"));

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Magika command failed"));
    }

    #[tokio::test]
    #[ignore = "benchmark"]
    async fn benchmark_mailbox_listing_and_push_paths() {
        fn generated_mailbox(index: usize) -> JmapMailbox {
            JmapMailbox {
                id: Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 + index as u128),
                role: String::new(),
                name: format!("Mailbox {index:04}"),
                sort_order: index as i32,
                total_emails: (index % 17) as u32,
                unread_emails: (index % 5) as u32,
            }
        }

        fn legacy_not_found(mailboxes: &[JmapMailbox], requested_ids: &[Uuid]) -> usize {
            requested_ids
                .iter()
                .filter(|id| !mailboxes.iter().any(|mailbox| mailbox.id == **id))
                .count()
        }

        fn optimized_not_found(mailbox_ids: &HashSet<Uuid>, requested_ids: &[Uuid]) -> usize {
            requested_ids
                .iter()
                .filter(|id| !mailbox_ids.contains(id))
                .count()
        }

        let account = FakeStore::account();
        let mailbox_count = 2_000usize;
        let mailboxes = (0..mailbox_count)
            .map(generated_mailbox)
            .collect::<Vec<_>>();
        let requested_ids = mailboxes
            .iter()
            .map(|mailbox| mailbox.id)
            .chain((0..mailbox_count).map(|index| {
                Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 + index as u128)
            }))
            .collect::<Vec<_>>();
        let mailbox_id_set = mailboxes
            .iter()
            .map(|mailbox| mailbox.id)
            .collect::<HashSet<_>>();

        let legacy_start = Instant::now();
        let mut legacy_missing = 0usize;
        for _ in 0..200 {
            legacy_missing = legacy_not_found(&mailboxes, &requested_ids);
        }
        let legacy_elapsed = legacy_start.elapsed();

        let optimized_start = Instant::now();
        let mut optimized_missing = 0usize;
        for _ in 0..200 {
            optimized_missing = optimized_not_found(&mailbox_id_set, &requested_ids);
        }
        let optimized_elapsed = optimized_start.elapsed();

        assert_eq!(legacy_missing, mailbox_count);
        assert_eq!(optimized_missing, mailbox_count);

        let store = FakeStore {
            session: Some(account.clone()),
            mailboxes: mailboxes.clone(),
            accessible_mailbox_accounts: std::iter::once(FakeStore::mailbox_access())
                .chain((0..32).map(|index| MailboxAccountAccess {
                    account_id: Uuid::from_u128(
                        0x3000_0000_0000_0000_0000_0000_0000_0000 + index as u128,
                    ),
                    email: format!("shared-{index}@example.test"),
                    display_name: format!("Shared {index}"),
                    is_owned: false,
                    may_read: true,
                    may_write: true,
                    may_send_as: true,
                    may_send_on_behalf: true,
                }))
                .collect(),
            ..Default::default()
        };
        let service = JmapService::new(store);
        let query_arguments = json!({
            "accountId": account.account_id.to_string(),
            "position": 0,
            "limit": 256,
        });
        let get_arguments = json!({
            "accountId": account.account_id.to_string(),
            "ids": requested_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "properties": ["id"],
        });

        let mailbox_query_start = Instant::now();
        for _ in 0..200 {
            service
                .handle_mailbox_query(&account, query_arguments.clone())
                .await
                .unwrap();
        }
        let mailbox_query_elapsed = mailbox_query_start.elapsed();

        let mailbox_get_start = Instant::now();
        for _ in 0..60 {
            service
                .handle_mailbox_get(&account, get_arguments.clone())
                .await
                .unwrap();
        }
        let mailbox_get_elapsed = mailbox_get_start.elapsed();

        let enabled_types = HashSet::from([
            "Mailbox".to_string(),
            "Email".to_string(),
            "Thread".to_string(),
        ]);
        let last_type_states = service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [account.account_id]);

        let push_start = Instant::now();
        for _ in 0..100 {
            service
                .compute_push_changes(account.account_id, &subscription, &change_set)
                .await
                .unwrap();
        }
        let push_elapsed = push_start.elapsed();

        println!(
            "BENCH lpe-jmap mailbox_get_not_found_reconciliation legacy={:?} optimized={:?} requested_ids={} mailboxes={}",
            legacy_elapsed,
            optimized_elapsed,
            requested_ids.len(),
            mailboxes.len()
        );
        println!(
            "BENCH lpe-jmap mailbox_query total={:?} avg_per_iter_us={} mailboxes={} limit={}",
            mailbox_query_elapsed,
            mailbox_query_elapsed.as_micros() / 200,
            mailboxes.len(),
            256
        );
        println!(
            "BENCH lpe-jmap mailbox_get total={:?} avg_per_iter_us={} requested_ids={}",
            mailbox_get_elapsed,
            mailbox_get_elapsed.as_micros() / 60,
            requested_ids.len()
        );
        println!(
            "BENCH lpe-jmap push_recompute total={:?} avg_per_iter_us={} visible_accounts={} types=3",
            push_elapsed,
            push_elapsed.as_micros() / 100,
            32
        );
    }
}

use std::sync::{Arc, Mutex};

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use base64::Engine as _;
use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
use lpe_mail_auth::{issue_oauth_access_token, AccountAuthStore, StoreFuture};
use lpe_storage::{
    AccountLogin, AuditEntryInput, AuthenticatedAccount, ImapEmail, JmapEmailAddress,
    JmapEmailQuery, JmapMailbox, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, SavedDraftMessage, SenderDelegationGrant,
    SenderDelegationGrantInput, SenderDelegationRight, SubmitMessageInput,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};
use uuid::Uuid;

use crate::{store::ImapStore, ImapServer};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone)]
struct FakeDetector;

impl Detector for FakeDetector {
    fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
        Ok(MagikaDetection {
            label: "pdf".to_string(),
            mime_type: "application/pdf".to_string(),
            description: "pdf".to_string(),
            group: "document".to_string(),
            extensions: vec!["pdf".to_string()],
            score: Some(0.99),
        })
    }
}

#[derive(Clone)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    login: AccountLogin,
    mailboxes: Arc<Mutex<Vec<JmapMailbox>>>,
    emails: Arc<Mutex<Vec<ImapEmail>>>,
    highest_modseq: Arc<Mutex<u64>>,
    mailbox_grants: Arc<Mutex<Vec<MailboxDelegationGrant>>>,
    sender_grants: Arc<Mutex<Vec<SenderDelegationGrant>>>,
    post_flag_update_action: Arc<Mutex<Option<PostFlagUpdateAction>>>,
}

impl FakeStore {
    fn new() -> Self {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        Self {
            session: None,
            login: AccountLogin {
                tenant_id: "tenant-a".to_string(),
                account_id,
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                password_hash: password_hash(),
                status: "active".to_string(),
            },
            mailboxes: Arc::new(Mutex::new(vec![
                mailbox("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "inbox", "Inbox", 0),
                mailbox("cccccccc-cccc-cccc-cccc-cccccccccccc", "sent", "Sent", 20),
                mailbox(
                    "dddddddd-dddd-dddd-dddd-dddddddddddd",
                    "drafts",
                    "Drafts",
                    10,
                ),
            ])),
            emails: Arc::new(Mutex::new(vec![
                email(
                    "11111111-1111-1111-1111-111111111111",
                    1,
                    2,
                    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "inbox",
                    "Inbox",
                    "Welcome",
                    true,
                    false,
                ),
                email(
                    "22222222-2222-2222-2222-222222222222",
                    2,
                    3,
                    "cccccccc-cccc-cccc-cccc-cccccccccccc",
                    "sent",
                    "Sent",
                    "Sent copy",
                    false,
                    true,
                ),
            ])),
            highest_modseq: Arc::new(Mutex::new(3)),
            mailbox_grants: Arc::new(Mutex::new(Vec::new())),
            sender_grants: Arc::new(Mutex::new(Vec::new())),
            post_flag_update_action: Arc::new(Mutex::new(None)),
        }
    }

    fn next_modseq(&self) -> u64 {
        let mut highest_modseq = self.highest_modseq.lock().unwrap();
        *highest_modseq += 1;
        *highest_modseq
    }

    fn enqueue_post_flag_update_action(&self, action: PostFlagUpdateAction) {
        *self.post_flag_update_action.lock().unwrap() = Some(action);
    }

    fn apply_post_flag_update_action(&self, action: PostFlagUpdateAction) {
        let mut emails = self.emails.lock().unwrap();
        match action {
            PostFlagUpdateAction::RemoveMessage { message_id } => {
                emails.retain(|email| email.id != message_id);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum PostFlagUpdateAction {
    RemoveMessage { message_id: Uuid },
}

impl AccountAuthStore for FakeStore {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        let session = if token == "session-token" {
            self.session.clone()
        } else {
            None
        };
        Box::pin(async move { Ok(session) })
    }

    fn fetch_account_login<'a>(&'a self, email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        let login = if email.eq_ignore_ascii_case(&self.login.email) {
            Some(self.login.clone())
        } else {
            None
        };
        Box::pin(async move { Ok(login) })
    }

    fn fetch_active_account_app_passwords<'a>(
        &'a self,
        _email: &'a str,
    ) -> StoreFuture<'a, Vec<lpe_storage::StoredAccountAppPassword>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn touch_account_app_password<'a>(
        &'a self,
        _email: &'a str,
        _app_password_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn append_audit_event<'a>(
        &'a self,
        _tenant_id: &'a str,
        _entry: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl ImapStore for FakeStore {
    fn ensure_imap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self.mailboxes.lock().unwrap().clone();
        Box::pin(async move { Ok(mailboxes) })
    }

    fn fetch_imap_highest_modseq<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, u64> {
        let highest_modseq = *self.highest_modseq.lock().unwrap();
        Box::pin(async move { Ok(highest_modseq) })
    }

    fn fetch_imap_emails<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
    ) -> StoreFuture<'a, Vec<ImapEmail>> {
        let mut emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == mailbox_id)
            .cloned()
            .collect::<Vec<_>>();
        emails.sort_by_key(|email| email.uid);
        Box::pin(async move { Ok(emails) })
    }

    fn update_imap_flags<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &'a [Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
        unchanged_since: Option<u64>,
    ) -> StoreFuture<'a, Vec<Uuid>> {
        let mut emails = self.emails.lock().unwrap();
        let modified_ids = emails
            .iter()
            .filter(|email| {
                email.mailbox_id == mailbox_id
                    && message_ids.contains(&email.id)
                    && unchanged_since.is_some_and(|limit| email.modseq > limit)
            })
            .map(|email| email.id)
            .collect::<Vec<_>>();
        let next_modseq = if modified_ids.len() == message_ids.len() {
            None
        } else {
            Some(self.next_modseq())
        };
        for email in emails.iter_mut() {
            if email.mailbox_id != mailbox_id
                || !message_ids.contains(&email.id)
                || modified_ids.contains(&email.id)
            {
                continue;
            }
            if let Some(unread) = unread {
                email.unread = unread;
            }
            if let Some(flagged) = flagged {
                email.flagged = flagged;
            }
            if let Some(modseq) = next_modseq {
                email.modseq = modseq;
            }
        }
        if let Some(action) = self.post_flag_update_action.lock().unwrap().take() {
            drop(emails);
            self.apply_post_flag_update_action(action);
        }
        Box::pin(async move { Ok(modified_ids) })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        _position: u64,
        _limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        let query = search_text.unwrap_or_default().to_ascii_lowercase();
        let ids = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
            .filter(|email| {
                query.is_empty()
                    || email.subject.to_ascii_lowercase().contains(&query)
                    || email.body_text.to_ascii_lowercase().contains(&query)
            })
            .map(|email| email.id)
            .collect::<Vec<_>>();
        let total = ids.len() as u64;
        Box::pin(async move { Ok(JmapEmailQuery { ids, total }) })
    }

    fn create_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        let mailbox = mailbox(
            &Uuid::new_v4().to_string(),
            "custom",
            name,
            mailboxes
                .iter()
                .map(|item| item.sort_order)
                .max()
                .unwrap_or(0)
                + 1,
        );
        let created = mailbox.clone();
        let _ = account_id;
        mailboxes.push(mailbox);
        Box::pin(async move { Ok(created) })
    }

    fn rename_imap_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        name: &'a str,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        let mailbox = mailboxes
            .iter_mut()
            .find(|mailbox| mailbox.id == mailbox_id)
            .unwrap();
        mailbox.name = name.to_string();
        let renamed = mailbox.clone();
        Box::pin(async move { Ok(renamed) })
    }

    fn delete_imap_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        let emails = self.emails.lock().unwrap();
        if emails.iter().any(|email| email.mailbox_id == mailbox_id) {
            return Box::pin(async move { anyhow::bail!("mailbox is not empty") });
        }
        mailboxes.retain(|mailbox| mailbox.id != mailbox_id);
        Box::pin(async move { Ok(()) })
    }

    fn copy_imap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail> {
        let mailboxes = self.mailboxes.lock().unwrap();
        let target_mailbox = mailboxes
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .unwrap()
            .clone();
        drop(mailboxes);

        let mut emails = self.emails.lock().unwrap();
        let modseq = self.next_modseq();
        let source = emails
            .iter()
            .find(|email| email.id == message_id)
            .unwrap()
            .clone();
        let next_uid = emails.iter().map(|email| email.uid).max().unwrap_or(0) + 1;
        let copied = ImapEmail {
            id: Uuid::new_v4(),
            uid: next_uid,
            modseq,
            thread_id: source.thread_id,
            mailbox_id: target_mailbox.id,
            mailbox_role: target_mailbox.role,
            mailbox_name: target_mailbox.name,
            received_at: source.received_at,
            sent_at: source.sent_at,
            from_address: source.from_address,
            from_display: source.from_display,
            to: source.to,
            cc: source.cc,
            bcc: source.bcc,
            subject: source.subject,
            preview: source.preview,
            body_text: source.body_text,
            body_html_sanitized: source.body_html_sanitized,
            unread: source.unread,
            flagged: source.flagged,
            has_attachments: source.has_attachments,
            size_octets: source.size_octets,
            internet_message_id: source.internet_message_id,
            delivery_status: "stored".to_string(),
        };
        emails.push(copied.clone());
        Box::pin(async move { Ok(copied) })
    }

    fn move_imap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail> {
        let mailboxes = self.mailboxes.lock().unwrap();
        let target_mailbox = mailboxes
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .unwrap()
            .clone();
        drop(mailboxes);

        let mut emails = self.emails.lock().unwrap();
        let modseq = self.next_modseq();
        let next_uid = emails.iter().map(|email| email.uid).max().unwrap_or(0) + 1;
        let moved = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        moved.mailbox_id = target_mailbox.id;
        moved.mailbox_role = target_mailbox.role;
        moved.mailbox_name = target_mailbox.name;
        moved.uid = next_uid;
        moved.modseq = modseq;
        let moved = moved.clone();
        Box::pin(async move { Ok(moved) })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        let mailboxes = self.mailboxes.lock().unwrap();
        let mailbox = mailboxes
            .iter()
            .find(|mailbox| mailbox.role == "drafts")
            .unwrap()
            .clone();
        drop(mailboxes);
        let mut emails = self.emails.lock().unwrap();
        let next_uid = emails.iter().map(|email| email.uid).max().unwrap_or(0) + 1;
        let modseq = self.next_modseq();
        let message_id = Uuid::new_v4();
        emails.push(ImapEmail {
            id: message_id,
            uid: next_uid,
            modseq,
            thread_id: Uuid::new_v4(),
            mailbox_id: mailbox.id,
            mailbox_role: mailbox.role,
            mailbox_name: mailbox.name,
            received_at: "2026-04-19T10:00:00Z".to_string(),
            sent_at: None,
            from_address: input.from_address,
            from_display: input.from_display,
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
            bcc: input
                .bcc
                .into_iter()
                .map(|recipient| JmapEmailAddress {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect(),
            subject: input.subject,
            preview: "draft".to_string(),
            body_text: input.body_text,
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            has_attachments: !input.attachments.is_empty(),
            size_octets: input.size_octets,
            internet_message_id: input.internet_message_id,
            delivery_status: "draft".to_string(),
        });
        let account_id = self.login.account_id;
        Box::pin(async move {
            Ok(SavedDraftMessage {
                message_id,
                account_id,
                submitted_by_account_id: account_id,
                draft_mailbox_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                delivery_status: "draft".to_string(),
            })
        })
    }

    fn fetch_account_identity<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, MailboxAccountAccess> {
        let identity = if account_id == self.login.account_id {
            MailboxAccountAccess {
                account_id: self.login.account_id,
                email: self.login.email.clone(),
                display_name: self.login.display_name.clone(),
                is_owned: true,
                may_read: true,
                may_write: true,
                may_send_as: true,
                may_send_on_behalf: false,
            }
        } else {
            panic!("unexpected account lookup");
        };
        Box::pin(async move { Ok(identity) })
    }

    fn fetch_outgoing_mailbox_delegation_grants<'a>(
        &'a self,
        owner_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MailboxDelegationGrant>> {
        assert_eq!(owner_account_id, self.login.account_id);
        let grants = self.mailbox_grants.lock().unwrap().clone();
        Box::pin(async move { Ok(grants) })
    }

    fn fetch_outgoing_sender_delegation_grants<'a>(
        &'a self,
        owner_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SenderDelegationGrant>> {
        assert_eq!(owner_account_id, self.login.account_id);
        let grants = self.sender_grants.lock().unwrap().clone();
        Box::pin(async move { Ok(grants) })
    }

    fn upsert_mailbox_delegation_grant<'a>(
        &'a self,
        input: MailboxDelegationGrantInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, MailboxDelegationGrant> {
        let mut grants = self.mailbox_grants.lock().unwrap();
        let normalized_email = input.grantee_email.trim().to_ascii_lowercase();
        let grantee_account_id = fake_grantee_account_id(&normalized_email);
        let grant = grants
            .iter_mut()
            .find(|grant| grant.grantee_email.eq_ignore_ascii_case(&normalized_email))
            .map(|grant| {
                grant.updated_at = "2026-04-22T10:05:00Z".to_string();
                grant.clone()
            })
            .unwrap_or_else(|| {
                let created = MailboxDelegationGrant {
                    id: Uuid::new_v4(),
                    owner_account_id: input.owner_account_id,
                    owner_email: self.login.email.clone(),
                    owner_display_name: self.login.display_name.clone(),
                    grantee_account_id,
                    grantee_email: normalized_email.clone(),
                    grantee_display_name: normalized_email.clone(),
                    created_at: "2026-04-22T10:00:00Z".to_string(),
                    updated_at: "2026-04-22T10:00:00Z".to_string(),
                };
                grants.push(created.clone());
                created
            });
        Box::pin(async move { Ok(grant) })
    }

    fn delete_mailbox_delegation_grant<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        assert_eq!(owner_account_id, self.login.account_id);
        let mut grants = self.mailbox_grants.lock().unwrap();
        let before = grants.len();
        grants.retain(|grant| grant.grantee_account_id != grantee_account_id);
        if grants.len() == before {
            return Box::pin(async move { anyhow::bail!("mailbox delegation grant not found") });
        }
        Box::pin(async move { Ok(()) })
    }

    fn upsert_sender_delegation_grant<'a>(
        &'a self,
        input: SenderDelegationGrantInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, SenderDelegationGrant> {
        let mut grants = self.sender_grants.lock().unwrap();
        let normalized_email = input.grantee_email.trim().to_ascii_lowercase();
        let grantee_account_id = fake_grantee_account_id(&normalized_email);
        let sender_right = input.sender_right.as_str().to_string();
        let grant = grants
            .iter_mut()
            .find(|grant| {
                grant.grantee_email.eq_ignore_ascii_case(&normalized_email)
                    && grant.sender_right == sender_right
            })
            .map(|grant| {
                grant.updated_at = "2026-04-22T10:05:00Z".to_string();
                grant.clone()
            })
            .unwrap_or_else(|| {
                let created = SenderDelegationGrant {
                    id: Uuid::new_v4(),
                    owner_account_id: input.owner_account_id,
                    owner_email: self.login.email.clone(),
                    owner_display_name: self.login.display_name.clone(),
                    grantee_account_id,
                    grantee_email: normalized_email.clone(),
                    grantee_display_name: normalized_email.clone(),
                    sender_right,
                    created_at: "2026-04-22T10:00:00Z".to_string(),
                    updated_at: "2026-04-22T10:00:00Z".to_string(),
                };
                grants.push(created.clone());
                created
            });
        Box::pin(async move { Ok(grant) })
    }

    fn delete_sender_delegation_grant<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        assert_eq!(owner_account_id, self.login.account_id);
        let mut grants = self.sender_grants.lock().unwrap();
        let before = grants.len();
        grants.retain(|grant| {
            !(grant.grantee_account_id == grantee_account_id
                && grant.sender_right == sender_right.as_str())
        });
        if grants.len() == before {
            return Box::pin(async move { anyhow::bail!("sender delegation grant not found") });
        }
        Box::pin(async move { Ok(()) })
    }
}

#[tokio::test]
async fn login_list_select_fetch_store_search_and_append_work() {
    let store = FakeStore::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let greeting = read_response(&mut stream, None).await;
    assert!(greeting.contains("* OK LPE IMAP ready"));

    let capability = send_command(&mut stream, "A0 CAPABILITY\r\n", "A0").await;
    assert!(capability.contains("CONDSTORE"));
    assert!(capability.contains("ID"));
    assert!(capability.contains("IDLE"));
    assert!(capability.contains("MOVE"));
    assert!(capability.contains("NAMESPACE"));
    assert!(capability.contains("SPECIAL-USE"));
    assert!(capability.contains("UIDPLUS"));

    let login = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    assert!(login.contains("A1 OK LOGIN completed"));

    let namespace = send_command(&mut stream, "A1B NAMESPACE\r\n", "A1B").await;
    assert!(namespace.contains("* NAMESPACE ((\"\" \"/\")) NIL NIL"));

    let list = send_command(&mut stream, "A2 LIST \"\" \"*\"\r\n", "A2").await;
    assert!(list.contains("* LIST (\\HasNoChildren) \"/\" \"INBOX\""));
    assert!(list.contains("* LIST (\\HasNoChildren \\Sent) \"/\" \"Sent\""));
    assert!(list.contains("* LIST (\\HasNoChildren \\Drafts) \"/\" \"Drafts\""));

    let select = send_command(&mut stream, "A3 SELECT Inbox\r\n", "A3").await;
    assert!(select.contains("* 1 EXISTS"));
    assert!(select.contains("* OK [HIGHESTMODSEQ 3]"));
    assert!(select.contains("A3 OK [READ-WRITE] SELECT completed"));

    let fetch = send_command(
        &mut stream,
        "A4 FETCH 1 (UID FLAGS BODY.PEEK[HEADER] BODY.PEEK[TEXT])\r\n",
        "A4",
    )
    .await;
    assert!(fetch.contains("UID 1"));
    assert!(fetch.contains("Subject: Welcome"));
    assert!(fetch.contains("Body Welcome"));

    let store_response = send_command(
        &mut stream,
        "A5 STORE 1 +FLAGS (\\Seen \\Flagged)\r\n",
        "A5",
    )
    .await;
    assert!(store_response.contains("* 1 FETCH (FLAGS (\\Seen \\Flagged))"));

    let fetch_modseq = send_command(&mut stream, "A5B FETCH 1 (UID MODSEQ)\r\n", "A5B").await;
    assert!(fetch_modseq.contains("UID 1"));
    assert!(fetch_modseq.contains("MODSEQ (4)"));

    let search = send_command(&mut stream, "A6 SEARCH SEEN TEXT Welcome\r\n", "A6").await;
    assert!(search.contains("* SEARCH 1"));

    let create_projects = send_command(&mut stream, "A7 CREATE Projects\r\n", "A7").await;
    assert!(create_projects.contains("A7 OK CREATE completed"));

    let create_deleted_items =
        send_command(&mut stream, "A7B CREATE \"Deleted Items\"\r\n", "A7B").await;
    assert!(create_deleted_items.contains("A7B OK CREATE completed"));

    let create_junk_email = send_command(&mut stream, "A7C CREATE \"Junk Email\"\r\n", "A7C").await;
    assert!(create_junk_email.contains("A7C OK CREATE completed"));

    let list_custom = send_command(&mut stream, "A8 LIST \"\" \"*\"\r\n", "A8").await;
    assert!(list_custom.contains("\"Projects\""));
    assert!(list_custom.contains("\"Deleted Items\""));
    assert!(list_custom.contains("\"Junk Email\""));

    let create_temp = send_command(&mut stream, "A8B CREATE Temp\r\n", "A8B").await;
    assert!(create_temp.contains("A8B OK CREATE completed"));
    let delete_temp = send_command(&mut stream, "A8C DELETE Temp\r\n", "A8C").await;
    assert!(delete_temp.contains("A8C OK DELETE completed"));

    let status = send_command(
        &mut stream,
        "A9 STATUS Projects (MESSAGES UIDNEXT UIDVALIDITY UNSEEN HIGHESTMODSEQ)\r\n",
        "A9",
    )
    .await;
    assert!(status.contains(
        "* STATUS \"Projects\" (MESSAGES 0 UIDNEXT 1 UIDVALIDITY 1 UNSEEN 0 HIGHESTMODSEQ 4)"
    ));

    let rename_projects = send_command(&mut stream, "A10 RENAME Projects Archive\r\n", "A10").await;
    assert!(rename_projects.contains("A10 OK RENAME completed"));

    let copy = send_command(&mut stream, "A11 COPY 1 Archive\r\n", "A11").await;
    assert!(copy.contains("A11 OK [COPYUID 1 1 3] COPY completed"));

    let select_archive = send_command(&mut stream, "A12 SELECT Archive\r\n", "A12").await;
    assert!(select_archive.contains("* 1 EXISTS"));

    let richer_search = send_command(
        &mut stream,
        "A13 SEARCH HEADER SUBJECT Welcome SINCE 19-Apr-2026 SMALLER 100\r\n",
        "A13",
    )
    .await;
    assert!(richer_search.contains("* SEARCH 1"));

    let move_response = send_command(&mut stream, "A14 UID MOVE 3 Inbox\r\n", "A14").await;
    assert!(move_response.contains("* 1 EXPUNGE"));
    assert!(move_response.contains("* 0 EXISTS"));
    assert!(move_response.contains("A14 OK [COPYUID 1 3 4] MOVE completed"));

    let select_archive_after_move =
        send_command(&mut stream, "A15 SELECT Archive\r\n", "A15").await;
    assert!(select_archive_after_move.contains("* 0 EXISTS"));

    let select_drafts = send_command(&mut stream, "A16 SELECT Drafts\r\n", "A16").await;
    assert!(select_drafts.contains("* 0 EXISTS"));

    let append_prelude = send_partial_command(&mut stream, "A17 APPEND Drafts {80}\r\n").await;
    assert!(append_prelude.contains("+ Ready for literal data"));
    let append = send_command(
        &mut stream,
        concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Draft\r\n",
            "\r\n",
            "Draft body\r\n",
            "\r\n"
        ),
        "A17",
    )
    .await;
    assert!(append.contains("A17 OK [APPENDUID 1 5] APPEND completed"));

    let drafts = send_command(&mut stream, "A18 UID SEARCH TEXT Draft\r\n", "A18").await;
    assert!(drafts.contains("* SEARCH 5"));

    let sent_literal = concat!(
        "From: Alice <alice@example.test>\r\n",
        "To: Bob <bob@example.test>\r\n",
        "Subject: Outlook sent append\r\n",
        "\r\n",
        "Sent body"
    );
    let append_sent_prelude = send_partial_command(
        &mut stream,
        &format!(
            "A18S APPEND Sent (\\Seen) \" 2-May-2026 21:44:00 +0200\" {{{}}}\r\n",
            sent_literal.as_bytes().len()
        ),
    )
    .await;
    assert!(append_sent_prelude.contains("+ Ready for literal data"));
    let append_sent = send_command(&mut stream, &format!("{sent_literal}\r\n"), "A18S").await;
    assert!(append_sent.contains("A18S OK APPEND completed"));
    assert!(!append_sent.contains("APPENDUID"));
    let sent_status = send_command(&mut stream, "A18T STATUS Sent (MESSAGES)\r\n", "A18T").await;
    assert!(sent_status.contains("* STATUS \"Sent\" (MESSAGES 1)"));

    let delete_archive = send_command(&mut stream, "A19 DELETE Archive\r\n", "A19").await;
    assert!(delete_archive.contains("A19 OK DELETE completed"));

    let select_inbox = send_command(&mut stream, "A20 SELECT Inbox\r\n", "A20").await;
    assert!(select_inbox.contains("* 2 EXISTS"));
    let rejected_move = send_command(&mut stream, "A21 MOVE 1 Drafts\r\n", "A21").await;
    assert!(rejected_move.contains("A21 NO MOVE does not support Sent or Drafts"));

    task.abort();
}

#[tokio::test]
async fn outlook_first_login_list_select_sync_transcript() {
    let store = FakeStore::new();
    {
        let mut emails = store.emails.lock().unwrap();
        emails[0].body_html_sanitized = Some("<p>Body Welcome</p>".to_string());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let greeting = read_response(&mut stream, None).await;
    assert!(greeting.contains("* OK LPE IMAP ready"));

    let capability = send_command(&mut stream, "OL1 CAPABILITY\r\n", "OL1").await;
    assert!(capability.contains("ID"));
    assert!(capability.contains("SPECIAL-USE"));
    assert!(capability.contains("UNSELECT"));

    let id = send_command(
        &mut stream,
        "OL2 ID (\"name\" \"Microsoft Outlook\" \"version\" \"16.0\")\r\n",
        "OL2",
    )
    .await;
    assert!(id.contains("* ID (\"name\" \"LPE\" \"vendor\" \"LPE\")"));
    assert!(id.contains("OL2 OK ID completed"));

    let login = send_command(
        &mut stream,
        "OL3 LOGIN alice@example.test secret\r\n",
        "OL3",
    )
    .await;
    assert!(login.contains("OL3 OK LOGIN completed"));

    let namespace = send_command(&mut stream, "OL4 NAMESPACE\r\n", "OL4").await;
    assert!(namespace.contains("* NAMESPACE ((\"\" \"/\")) NIL NIL"));

    let subscribe = send_command(&mut stream, "OL5 SUBSCRIBE Inbox\r\n", "OL5").await;
    assert!(subscribe.contains("OL5 OK SUBSCRIBE completed"));

    let lsub = send_command(&mut stream, "OL6 LSUB \"\" \"*\"\r\n", "OL6").await;
    assert!(lsub.contains("* LSUB (\\HasNoChildren) \"/\" \"INBOX\""));
    assert!(lsub.contains("* LSUB (\\HasNoChildren \\Sent) \"/\" \"Sent\""));
    assert!(lsub.contains("* LSUB (\\HasNoChildren \\Drafts) \"/\" \"Drafts\""));

    let unsubscribe = send_command(&mut stream, "OL7 UNSUBSCRIBE Inbox\r\n", "OL7").await;
    assert!(unsubscribe.contains("OL7 OK UNSUBSCRIBE completed"));

    let list = send_command(&mut stream, "OL8 LIST \"\" \"*\"\r\n", "OL8").await;
    assert!(list.contains("* LIST (\\HasNoChildren) \"/\" \"INBOX\""));
    assert!(list.contains("* LIST (\\HasNoChildren \\Sent) \"/\" \"Sent\""));
    assert!(list.contains("* LIST (\\HasNoChildren \\Drafts) \"/\" \"Drafts\""));

    let list_inbox = send_command(&mut stream, "OL8A LIST \"\" \"INBOX\"\r\n", "OL8A").await;
    assert_eq!(list_inbox.matches("* LIST ").count(), 1);
    assert!(list_inbox.contains("* LIST (\\HasNoChildren) \"/\" \"INBOX\""));
    assert!(!list_inbox.contains("\"Sent\""));
    assert!(!list_inbox.contains("\"Drafts\""));

    let list_root = send_command(&mut stream, "OL8B LIST \"\" \"\"\r\n", "OL8B").await;
    assert!(list_root.contains("* LIST (\\Noselect) \"/\" \"\""));

    let xlist = send_command(&mut stream, "OL8X XLIST \"\" \"*\"\r\n", "OL8X").await;
    assert!(xlist.contains("* XLIST (\\HasNoChildren \\Inbox) \"/\" \"INBOX\""));
    assert!(xlist.contains("OL8X OK XLIST completed"));

    let examine = send_command(&mut stream, "OL8E EXAMINE Inbox\r\n", "OL8E").await;
    assert!(examine.contains("OL8E OK [READ-ONLY] EXAMINE completed"));

    let examine_body = send_command(&mut stream, "OL8F UID FETCH 1 (BODY[])\r\n", "OL8F").await;
    assert!(examine_body.contains("BODY[]"));
    assert!(examine_body.contains("Content-Type: multipart/alternative; boundary=\"lpe-alt-"));
    assert!(examine_body.contains("Content-Type: text/plain; charset=UTF-8"));
    assert!(examine_body.contains("Content-Type: text/html; charset=UTF-8"));
    assert!(examine_body.contains("--lpe-alt-"));
    assert!(store.emails.lock().unwrap()[0].unread);

    let unselect = send_command(&mut stream, "OL8U UNSELECT\r\n", "OL8U").await;
    assert!(unselect.contains("OL8U OK UNSELECT completed"));

    let select = send_command(&mut stream, "OL9 SELECT Inbox\r\n", "OL9").await;
    assert!(select.contains("* 1 EXISTS"));
    assert!(select.contains("* OK [UIDVALIDITY 1]"));
    assert!(select.contains("* OK [UIDNEXT 2]"));
    assert!(select.contains("* OK [HIGHESTMODSEQ 3]"));

    let status = send_command(
        &mut stream,
        "OL9S STATUS INBOX (MESSAGES UIDNEXT UIDVALIDITY UNSEEN)\r\n",
        "OL9S",
    )
    .await;
    assert!(status.contains("* STATUS \"INBOX\""));
    assert!(status.contains("MESSAGES 1"));

    let fetch_summary = send_command(
        &mut stream,
        "OL10 UID FETCH 1:* (UID FLAGS INTERNALDATE RFC822.SIZE ENVELOPE BODYSTRUCTURE)\r\n",
        "OL10",
    )
    .await;
    assert!(fetch_summary.contains("UID 1"));
    assert!(fetch_summary.contains("ENVELOPE"));
    assert!(fetch_summary.contains("\"Welcome\""));
    assert!(fetch_summary.contains("BODYSTRUCTURE ((\"TEXT\" \"PLAIN\""));
    assert!(fetch_summary.contains("\"ALTERNATIVE\""));
    assert!(fetch_summary.contains("(\"BOUNDARY\" \"lpe-alt-"));

    let fetch_body = send_command(&mut stream, "OL10B UID FETCH 1 (UID BODY)\r\n", "OL10B").await;
    assert!(fetch_body.contains("UID 1 BODY ((\"TEXT\" \"PLAIN\""));

    let search_undeleted = send_command(
        &mut stream,
        "OL10C UID SEARCH CHARSET UTF-8 UNDELETED\r\n",
        "OL10C",
    )
    .await;
    assert!(search_undeleted.contains("* SEARCH 1"));

    let search_not_deleted =
        send_command(&mut stream, "OL10D UID SEARCH 1:* NOT DELETED\r\n", "OL10D").await;
    assert!(search_not_deleted.contains("* SEARCH 1"));

    let uid_fetch_flags =
        send_command(&mut stream, "OL10E UID FETCH 1:* (FLAGS)\r\n", "OL10E").await;
    assert!(uid_fetch_flags.contains("* 1 FETCH (UID 1 FLAGS ("));

    let search_unkeyword =
        send_command(&mut stream, "OL10F UID SEARCH UNKEYWORD $Junk\r\n", "OL10F").await;
    assert!(search_unkeyword.contains("* SEARCH 1"));

    let search_return_all = send_command(
        &mut stream,
        "OL10R UID SEARCH RETURN (ALL) CHARSET UTF-8 UNDELETED\r\n",
        "OL10R",
    )
    .await;
    assert!(search_return_all.contains("* SEARCH 1"));

    let uid_expunge = send_command(&mut stream, "OL10G UID EXPUNGE 1\r\n", "OL10G").await;
    assert!(uid_expunge.contains("OL10G OK UID EXPUNGE completed"));

    let fetch_headers = send_command(
        &mut stream,
        "OL11 UID FETCH 1 (BODY.PEEK[HEADER.FIELDS (DATE FROM TO SUBJECT MESSAGE-ID)])\r\n",
        "OL11",
    )
    .await;
    assert!(fetch_headers.contains("BODY.PEEK[HEADER.FIELDS (DATE FROM TO SUBJECT MESSAGE-ID)]"));
    assert!(fetch_headers.contains("Date: 19 Apr 2026 08:00:00 +0000"));
    assert!(fetch_headers.contains("Subject: Welcome"));
    assert!(
        fetch_headers.contains("Message-Id: <11111111-1111-1111-1111-111111111111@example.test>")
    );
    assert!(!fetch_headers.contains("\r\nBcc:"));

    let fetch_header_exclusion = send_command(
        &mut stream,
        "OL11B UID FETCH 1 (BODY.PEEK[HEADER.FIELDS.NOT (RECEIVED BCC)])\r\n",
        "OL11B",
    )
    .await;
    assert!(fetch_header_exclusion.contains("BODY.PEEK[HEADER.FIELDS.NOT (RECEIVED BCC)]"));
    assert!(fetch_header_exclusion.contains("Subject: Welcome"));
    assert!(!fetch_header_exclusion.contains("\r\nBcc:"));

    let fetch_part_headers = send_command(
        &mut stream,
        "OL11C UID FETCH 1 (BODY.PEEK[1.HEADER.FIELDS (CONTENT-TYPE)])\r\n",
        "OL11C",
    )
    .await;
    assert!(fetch_part_headers.contains("BODY.PEEK[1.HEADER.FIELDS (CONTENT-TYPE)]"));
    assert!(fetch_part_headers.contains("Content-Type: multipart/alternative"));

    let fetch_section =
        send_command(&mut stream, "OL12 UID FETCH 1 (BODY.PEEK[1])\r\n", "OL12").await;
    assert!(fetch_section.contains("BODY.PEEK[1]"));
    assert!(fetch_section.contains("Body Welcome"));

    let fetch_html_section =
        send_command(&mut stream, "OL12B UID FETCH 1 (BODY.PEEK[2])\r\n", "OL12B").await;
    assert!(fetch_html_section.contains("BODY.PEEK[2]"));
    assert!(fetch_html_section.contains("<p>Body Welcome</p>"));

    let fetch_partial = send_command(
        &mut stream,
        "OL13 UID FETCH 1 (BODY.PEEK[]<0.20>)\r\n",
        "OL13",
    )
    .await;
    assert!(fetch_partial.contains("BODY.PEEK[]<0> {20}"));
    assert!(fetch_partial.contains("Date: 19 Apr 2026"));

    let check = send_command(&mut stream, "OL14 CHECK\r\n", "OL14").await;
    assert!(check.contains("OL14 OK CHECK completed"));

    let expunge = send_command(&mut stream, "OL15 EXPUNGE\r\n", "OL15").await;
    assert!(expunge.contains("OL15 OK EXPUNGE completed"));

    let close = send_command(&mut stream, "OL16 CLOSE\r\n", "OL16").await;
    assert!(close.contains("OL16 OK CLOSE completed"));

    task.abort();
}

#[tokio::test]
async fn condstore_store_reports_modified_and_keeps_fresh_messages() {
    let store = FakeStore::new();
    let archive_id = Uuid::new_v4();
    {
        let mut mailboxes = store.mailboxes.lock().unwrap();
        mailboxes.push(JmapMailbox {
            id: archive_id,
            role: String::new(),
            name: "Archive".to_string(),
            sort_order: 30,
            total_emails: 0,
            unread_emails: 0,
        });
    }
    {
        let mut emails = store.emails.lock().unwrap();
        emails.push(email(
            "33333333-3333-3333-3333-333333333333",
            1,
            4,
            &archive_id.to_string(),
            "",
            "Archive",
            "First",
            true,
            false,
        ));
        emails.push(email(
            "44444444-4444-4444-4444-444444444444",
            2,
            5,
            &archive_id.to_string(),
            "",
            "Archive",
            "Second",
            true,
            false,
        ));
    }
    *store.highest_modseq.lock().unwrap() = 5;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    let _ = send_command(&mut stream, "A2 SELECT Archive\r\n", "A2").await;

    {
        let next_modseq = store.next_modseq();
        let mut emails = store.emails.lock().unwrap();
        let second = emails
            .iter_mut()
            .find(|email| email.subject == "Second")
            .unwrap();
        second.flagged = true;
        second.modseq = next_modseq;
    }

    let conditional_store = send_command(
        &mut stream,
        "A3 STORE 1:2 (UNCHANGEDSINCE 4) +FLAGS (\\Seen)\r\n",
        "A3",
    )
    .await;
    assert!(conditional_store.contains("* 1 FETCH (FLAGS (\\Seen))"));
    assert!(conditional_store.contains("A3 NO [MODIFIED 2] conditional STORE failed"));

    let fetch_after = send_command(&mut stream, "A4 FETCH 1:2 (FLAGS MODSEQ)\r\n", "A4").await;
    assert!(fetch_after.contains("* 1 FETCH (FLAGS (\\Seen) MODSEQ (7))"));
    assert!(fetch_after.contains("* 2 FETCH (FLAGS (\\Flagged) MODSEQ (6))"));

    task.abort();
}

#[tokio::test]
async fn inbox_fetch_and_search_do_not_leak_bcc() {
    let store = FakeStore::new();
    {
        let mut emails = store.emails.lock().unwrap();
        emails[0].bcc = vec![JmapEmailAddress {
            address: "hidden@example.test".to_string(),
            display_name: Some("Hidden".to_string()),
        }];
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store, Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _greeting = read_response(&mut stream, None).await;
    let login = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    assert!(login.contains("A1 OK LOGIN completed"));
    let select = send_command(&mut stream, "A2 SELECT Inbox\r\n", "A2").await;
    assert!(select.contains("A2 OK [READ-WRITE] SELECT completed"));

    let fetch = send_command(&mut stream, "A3 FETCH 1 (BODY.PEEK[HEADER])\r\n", "A3").await;
    assert!(!fetch.contains("\r\nBcc:"));

    let search = send_command(
        &mut stream,
        "A4 SEARCH HEADER BCC hidden@example.test\r\n",
        "A4",
    )
    .await;
    assert!(search.contains("* SEARCH "));
    assert!(!search.contains("hidden@example.test"));

    task.abort();
}

#[tokio::test]
async fn sent_fetch_reconstructs_owner_bcc_header() {
    let store = FakeStore::new();
    {
        let mut emails = store.emails.lock().unwrap();
        emails[1].bcc = vec![JmapEmailAddress {
            address: "hidden@example.test".to_string(),
            display_name: Some("Hidden".to_string()),
        }];
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store, Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _greeting = read_response(&mut stream, None).await;
    let login = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    assert!(login.contains("A1 OK LOGIN completed"));
    let select = send_command(&mut stream, "A2 SELECT Sent\r\n", "A2").await;
    assert!(select.contains("A2 OK [READ-WRITE] SELECT completed"));

    let fetch = send_command(&mut stream, "A3 FETCH 1 (BODY.PEEK[HEADER])\r\n", "A3").await;
    assert!(fetch.contains("\r\nBcc: Hidden <hidden@example.test>"));

    task.abort();
}

#[tokio::test]
async fn idle_reports_selected_mailbox_flag_changes() {
    let store = FakeStore::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    let _ = send_command(&mut stream, "A2 SELECT Inbox\r\n", "A2").await;

    let idle = send_partial_command(&mut stream, "A3 IDLE\r\n").await;
    assert!(idle.contains("+ idling"));

    {
        let next_modseq = store.next_modseq();
        let mut emails = store.emails.lock().unwrap();
        let inbox_email = emails
            .iter_mut()
            .find(|email| email.mailbox_name == "Inbox")
            .unwrap();
        inbox_email.unread = false;
        inbox_email.flagged = true;
        inbox_email.modseq = next_modseq;
    }

    let update = read_response_with_timeout(&mut stream, None, 2_500).await;
    assert!(update.contains("* 1 FETCH (FLAGS (\\Seen \\Flagged))"));

    let done = send_command(&mut stream, "DONE\r\n", "A3").await;
    assert!(done.contains("A3 OK IDLE completed"));

    task.abort();
}

#[tokio::test]
async fn store_survives_concurrent_selected_mailbox_removal() {
    let store = FakeStore::new();
    let archive_id = Uuid::new_v4();
    {
        let mut mailboxes = store.mailboxes.lock().unwrap();
        mailboxes.push(JmapMailbox {
            id: archive_id,
            role: String::new(),
            name: "Archive".to_string(),
            sort_order: 30,
            total_emails: 0,
            unread_emails: 0,
        });
    }
    {
        let mut emails = store.emails.lock().unwrap();
        emails.push(email(
            "33333333-3333-3333-3333-333333333333",
            3,
            4,
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "inbox",
            "Inbox",
            "Follow-up",
            true,
            false,
        ));
    }
    *store.highest_modseq.lock().unwrap() = 4;
    store.enqueue_post_flag_update_action(PostFlagUpdateAction::RemoveMessage {
        message_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    let _ = send_command(&mut stream, "A2 SELECT Inbox\r\n", "A2").await;

    let store_response =
        send_command(&mut stream, "A3 UID STORE 3 +FLAGS (\\Flagged)\r\n", "A3").await;
    assert!(store_response.contains("* 1 FETCH (FLAGS (\\Flagged))"));
    assert!(store_response.contains("A3 OK STORE completed"));

    let fetch_after = send_command(&mut stream, "A4 UID FETCH 3 (FLAGS MODSEQ)\r\n", "A4").await;
    assert!(fetch_after.contains("* 1 FETCH (UID 3 FLAGS (\\Flagged) MODSEQ (5))"));
    assert!(fetch_after.contains("A4 OK FETCH completed"));
    assert!(store
        .emails
        .lock()
        .unwrap()
        .iter()
        .any(|email| email.subject == "Follow-up" && email.flagged));

    task.abort();
}

#[tokio::test]
async fn idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change() {
    let store = FakeStore::new();
    let archive_id = Uuid::new_v4();
    {
        let mut mailboxes = store.mailboxes.lock().unwrap();
        mailboxes.push(JmapMailbox {
            id: archive_id,
            role: String::new(),
            name: "Archive".to_string(),
            sort_order: 30,
            total_emails: 0,
            unread_emails: 0,
        });
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    let _ = send_command(&mut stream, "A2 SELECT Inbox\r\n", "A2").await;

    let idle = send_partial_command(&mut stream, "A3 IDLE\r\n").await;
    assert!(idle.contains("+ idling"));

    {
        let moved_modseq = store.next_modseq();
        let replacement_modseq = store.next_modseq();
        let mut emails = store.emails.lock().unwrap();
        let moved = emails
            .iter_mut()
            .find(|email| email.mailbox_name == "Inbox")
            .unwrap();
        moved.mailbox_id = archive_id;
        moved.mailbox_role.clear();
        moved.mailbox_name = "Archive".to_string();
        moved.uid = 3;
        moved.modseq = moved_modseq;
        emails.push(email(
            "44444444-4444-4444-4444-444444444444",
            4,
            replacement_modseq,
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "inbox",
            "Inbox",
            "Replacement",
            true,
            false,
        ));
    }

    let update = read_response_with_timeout(&mut stream, None, 2_500).await;
    assert!(update.contains("* 1 EXPUNGE"));
    assert!(update.contains("* 1 EXISTS"));
    assert!(update.contains("* 1 FETCH (UID 4 FLAGS ())"));

    let done = send_command(&mut stream, "DONE\r\n", "A3").await;
    assert!(done.contains("A3 OK IDLE completed"));

    task.abort();
}

#[tokio::test]
async fn idle_without_selected_mailbox_is_noop_for_outlook() {
    let store = FakeStore::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;

    let idle = send_partial_command(&mut stream, "A2 IDLE\r\n").await;
    assert!(idle.contains("+ idling"));

    let done = send_command(&mut stream, "DONE\r\n", "A2").await;
    assert!(done.contains("A2 OK IDLE completed"));

    task.abort();
}

#[tokio::test]
async fn xoauth2_authenticate_is_accepted() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var(
        "LPE_MAIL_OAUTH_SIGNING_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let mut store = FakeStore::new();
    store.session = Some(AuthenticatedAccount {
        tenant_id: store.login.tenant_id.clone(),
        account_id: store.login.account_id,
        email: store.login.email.clone(),
        display_name: store.login.display_name.clone(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    });
    let token = issue_oauth_access_token(
        &lpe_mail_auth::AccountPrincipal {
            tenant_id: store.login.tenant_id.clone(),
            account_id: store.login.account_id,
            email: store.login.email.clone(),
            display_name: store.login.display_name.clone(),
        },
        "imap",
        600,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let auth_payload = base64::engine::general_purpose::STANDARD.encode(format!(
        "user={}\u{1}auth=Bearer {}\u{1}\u{1}",
        store.login.email, token
    ));

    let response = send_command(
        &mut stream,
        &format!("A1 AUTHENTICATE XOAUTH2 {auth_payload}\r\n"),
        "A1",
    )
    .await;

    assert!(response.contains("A1 OK AUTHENTICATE completed"));
    std::env::remove_var("LPE_MAIL_OAUTH_SIGNING_SECRET");
    task.abort();
}

#[tokio::test]
async fn acl_commands_project_canonical_mailbox_and_sender_delegation() {
    let store = FakeStore::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = ImapServer::with_validator(store.clone(), Validator::new(FakeDetector, 0.8));
    let task = tokio::spawn(async move { server.serve(listener).await.unwrap() });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let _ = read_response(&mut stream, None).await;
    let _ = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;

    let capability = send_command(&mut stream, "A2 CAPABILITY\r\n", "A2").await;
    assert!(capability.contains("ACL"));

    let setacl = send_command(
        &mut stream,
        "A3 SETACL Inbox bob@example.test lrswitepb\r\n",
        "A3",
    )
    .await;
    assert!(setacl.contains("A3 OK SETACL completed"));

    let getacl = send_command(&mut stream, "A4 GETACL Inbox\r\n", "A4").await;
    assert!(getacl.contains("* ACL \"Inbox\" alice@example.test lrswiteapb"));
    assert!(getacl.contains("bob@example.test lrswitepb"));

    let myrights = send_command(&mut stream, "A5 MYRIGHTS Inbox\r\n", "A5").await;
    assert!(myrights.contains("* MYRIGHTS \"Inbox\" lrswiteapb"));

    let listrights = send_command(
        &mut stream,
        "A6 LISTRIGHTS Inbox bob@example.test\r\n",
        "A6",
    )
    .await;
    assert!(listrights.contains("* LISTRIGHTS \"Inbox\" \"bob@example.test\" \"\" lrswiteapb"));

    let remove_send_as =
        send_command(&mut stream, "A7 SETACL Inbox bob@example.test -p\r\n", "A7").await;
    assert!(remove_send_as.contains("A7 OK SETACL completed"));

    let getacl_after_remove = send_command(&mut stream, "A8 GETACL Inbox\r\n", "A8").await;
    assert!(getacl_after_remove.contains("bob@example.test lrswiteb"));
    assert!(!getacl_after_remove.contains("bob@example.test lrswitepb"));

    let deleteacl =
        send_command(&mut stream, "A9 DELETEACL Inbox bob@example.test\r\n", "A9").await;
    assert!(deleteacl.contains("A9 OK DELETEACL completed"));

    let getacl_after_delete = send_command(&mut stream, "A10 GETACL Inbox\r\n", "A10").await;
    assert!(getacl_after_delete.contains("* ACL \"Inbox\" alice@example.test lrswiteapb"));
    assert!(!getacl_after_delete.contains("bob@example.test"));

    let invalid_send_only = send_command(
        &mut stream,
        "A11 SETACL Inbox bob@example.test p\r\n",
        "A11",
    )
    .await;
    assert!(
        invalid_send_only.contains("A11 NO sender delegation rights require mailbox access rights")
    );

    task.abort();
}

fn mailbox(id: &str, role: &str, name: &str, sort_order: i32) -> JmapMailbox {
    JmapMailbox {
        id: Uuid::parse_str(id).unwrap(),
        role: role.to_string(),
        name: name.to_string(),
        sort_order,
        total_emails: 0,
        unread_emails: 0,
    }
}

fn email(
    id: &str,
    uid: u32,
    modseq: u64,
    mailbox_id: &str,
    mailbox_role: &str,
    mailbox_name: &str,
    subject: &str,
    unread: bool,
    flagged: bool,
) -> ImapEmail {
    ImapEmail {
        id: Uuid::parse_str(id).unwrap(),
        uid,
        modseq,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::parse_str(mailbox_id).unwrap(),
        mailbox_role: mailbox_role.to_string(),
        mailbox_name: mailbox_name.to_string(),
        received_at: "2026-04-19T08:00:00Z".to_string(),
        sent_at: Some("2026-04-19T08:00:00Z".to_string()),
        from_address: "bob@example.test".to_string(),
        from_display: Some("Bob".to_string()),
        to: vec![JmapEmailAddress {
            address: "alice@example.test".to_string(),
            display_name: Some("Alice".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: subject.to_string(),
        preview: subject.to_string(),
        body_text: format!("Body {}", subject),
        body_html_sanitized: None,
        unread,
        flagged,
        has_attachments: false,
        size_octets: 64,
        internet_message_id: Some(format!("<{}@example.test>", id)),
        delivery_status: "stored".to_string(),
    }
}

fn password_hash() -> String {
    Argon2::default()
        .hash_password(b"secret", &SaltString::generate(&mut OsRng))
        .unwrap()
        .to_string()
}

fn fake_grantee_account_id(email: &str) -> Uuid {
    let mut bytes = [0u8; 16];
    for (index, byte) in email.as_bytes().iter().enumerate() {
        bytes[index % 16] ^= *byte;
    }
    Uuid::from_bytes(bytes)
}

async fn send_partial_command(stream: &mut TcpStream, value: &str) -> String {
    stream.write_all(value.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    read_response(stream, None).await
}

async fn send_command(stream: &mut TcpStream, command: &str, tag: &str) -> String {
    stream.write_all(command.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    read_response(stream, Some(tag)).await
}

async fn read_response(stream: &mut TcpStream, tag: Option<&str>) -> String {
    read_response_with_timeout(stream, tag, 500).await
}

async fn read_response_with_timeout(
    stream: &mut TcpStream,
    tag: Option<&str>,
    timeout_ms: u64,
) -> String {
    let mut buffer = vec![0u8; 4096];
    let mut output = String::new();
    loop {
        let bytes = timeout(Duration::from_millis(timeout_ms), stream.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        if bytes == 0 {
            break;
        }
        output.push_str(&String::from_utf8_lossy(&buffer[..bytes]));
        match tag {
            Some(tag)
                if output.contains(&format!("\r\n{tag} "))
                    || output.starts_with(&format!("{tag} ")) =>
            {
                break;
            }
            None if output.ends_with("\r\n") => break,
            _ => {}
        }
    }
    output
}

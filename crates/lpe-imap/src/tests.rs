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
    JmapEmailQuery, JmapMailbox, SavedDraftMessage, SubmitMessageInput,
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
                    "cccccccc-cccc-cccc-cccc-cccccccccccc",
                    "sent",
                    "Sent",
                    "Sent copy",
                    false,
                    true,
                ),
            ])),
        }
    }
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

    fn fetch_imap_emails<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
    ) -> StoreFuture<'a, Vec<ImapEmail>> {
        let emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == mailbox_id)
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(emails) })
    }

    fn update_imap_flags<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &'a [Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
    ) -> StoreFuture<'a, ()> {
        let mut emails = self.emails.lock().unwrap();
        for email in emails.iter_mut() {
            if email.mailbox_id != mailbox_id || !message_ids.contains(&email.id) {
                continue;
            }
            if let Some(unread) = unread {
                email.unread = unread;
            }
            if let Some(flagged) = flagged {
                email.flagged = flagged;
            }
        }
        Box::pin(async move { Ok(()) })
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
            "",
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
        let source = emails
            .iter()
            .find(|email| email.id == message_id)
            .unwrap()
            .clone();
        let next_uid = emails
            .iter()
            .filter(|email| email.mailbox_id == target_mailbox_id)
            .map(|email| email.uid)
            .max()
            .unwrap_or(0)
            + 1;
        let copied = ImapEmail {
            id: Uuid::new_v4(),
            uid: next_uid,
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
        let message_id = Uuid::new_v4();
        emails.push(ImapEmail {
            id: message_id,
            uid: next_uid,
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
                draft_mailbox_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                delivery_status: "draft".to_string(),
            })
        })
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

    let login = send_command(&mut stream, "A1 LOGIN alice@example.test secret\r\n", "A1").await;
    assert!(login.contains("A1 OK LOGIN completed"));

    let list = send_command(&mut stream, "A2 LIST \"\" \"*\"\r\n", "A2").await;
    assert!(list.contains("* LIST (\\Inbox) \"/\" \"Inbox\""));
    assert!(list.contains("* LIST (\\Sent) \"/\" \"Sent\""));
    assert!(list.contains("* LIST (\\Drafts) \"/\" \"Drafts\""));

    let select = send_command(&mut stream, "A3 SELECT Inbox\r\n", "A3").await;
    assert!(select.contains("* 1 EXISTS"));
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

    let search = send_command(&mut stream, "A6 SEARCH SEEN TEXT Welcome\r\n", "A6").await;
    assert!(search.contains("* SEARCH 1"));

    let create_projects = send_command(&mut stream, "A7 CREATE Projects\r\n", "A7").await;
    assert!(create_projects.contains("A7 OK CREATE completed"));

    let list_custom = send_command(&mut stream, "A8 LIST \"\" \"*\"\r\n", "A8").await;
    assert!(list_custom.contains("\"Projects\""));

    let create_temp = send_command(&mut stream, "A8B CREATE Temp\r\n", "A8B").await;
    assert!(create_temp.contains("A8B OK CREATE completed"));
    let delete_temp = send_command(&mut stream, "A8C DELETE Temp\r\n", "A8C").await;
    assert!(delete_temp.contains("A8C OK DELETE completed"));

    let status = send_command(
        &mut stream,
        "A9 STATUS Projects (MESSAGES UIDNEXT UIDVALIDITY UNSEEN)\r\n",
        "A9",
    )
    .await;
    assert!(status.contains("* STATUS \"Projects\" (MESSAGES 0 UIDNEXT 1 UIDVALIDITY 1 UNSEEN 0)"));

    let rename_projects = send_command(&mut stream, "A10 RENAME Projects Archive\r\n", "A10").await;
    assert!(rename_projects.contains("A10 OK RENAME completed"));

    let copy = send_command(&mut stream, "A11 COPY 1 Archive\r\n", "A11").await;
    assert!(copy.contains("A11 OK [COPYUID 1 1 1] COPY completed"));

    let select_archive = send_command(&mut stream, "A12 SELECT Archive\r\n", "A12").await;
    assert!(select_archive.contains("* 1 EXISTS"));

    let richer_search = send_command(
        &mut stream,
        "A13 SEARCH HEADER SUBJECT Welcome SINCE 19-Apr-2026 SMALLER 100\r\n",
        "A13",
    )
    .await;
    assert!(richer_search.contains("* SEARCH 1"));

    let select_drafts = send_command(&mut stream, "A14 SELECT Drafts\r\n", "A14").await;
    assert!(select_drafts.contains("* 0 EXISTS"));

    let append_prelude = send_partial_command(&mut stream, "A15 APPEND Drafts {80}\r\n").await;
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
        "A15",
    )
    .await;
    assert!(append.contains("A15 OK APPEND completed"));

    let drafts = send_command(&mut stream, "A16 UID SEARCH TEXT Draft\r\n", "A16").await;
    assert!(drafts.contains("* SEARCH 3"));

    let select_archive_again = send_command(&mut stream, "A17 SELECT Archive\r\n", "A17").await;
    assert!(select_archive_again.contains("* 1 EXISTS"));
    let delete_archive = send_command(&mut stream, "A18 DELETE Archive\r\n", "A18").await;
    assert!(delete_archive.contains("A18 NO mailbox is not empty"));

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
    let mut buffer = vec![0u8; 4096];
    let mut output = String::new();
    loop {
        let bytes = timeout(Duration::from_millis(500), stream.read(&mut buffer))
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

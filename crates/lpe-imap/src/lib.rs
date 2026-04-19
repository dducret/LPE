use anyhow::{anyhow, bail, Result};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{normalize_login_name, verify_password, AccountPrincipal};
use lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, ImapEmail, JmapEmailAddress, SubmitMessageInput,
};
use std::{collections::HashSet, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

mod store;

use crate::store::ImapStore;

const CAPABILITIES: &str = "IMAP4rev1 UIDPLUS";
const UID_VALIDITY: u32 = 1;

#[derive(Clone)]
pub struct ImapServer<S, D> {
    store: S,
    validator: Arc<Validator<D>>,
}

impl<S: ImapStore> ImapServer<S, lpe_magika::SystemDetector> {
    pub fn new(store: S) -> Self {
        Self::with_validator(store, Validator::from_env())
    }
}

impl<S: ImapStore, D: Detector> ImapServer<S, D> {
    pub fn with_validator(store: S, validator: Validator<D>) -> Self {
        Self {
            store,
            validator: Arc::new(validator),
        }
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                let _ = server.handle_connection(stream).await;
            });
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut session = Session::new(self.store.clone(), self.validator.clone());

        write_half
            .write_all(b"* OK LPE IMAP ready\r\n")
            .await?;
        write_half.flush().await?;

        loop {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                continue;
            }
            let keep_running = session
                .handle_request(&mut reader, &mut write_half, line)
                .await?;
            if !keep_running {
                break;
            }
        }

        Ok(())
    }
}

pub async fn serve(listener: TcpListener, store: impl ImapStore) -> Result<()> {
    ImapServer::new(store).serve(listener).await
}

#[derive(Clone)]
struct Session<S, D> {
    store: S,
    validator: Arc<Validator<D>>,
    principal: Option<AccountPrincipal>,
    selected: Option<SelectedMailbox>,
}

#[derive(Clone)]
struct SelectedMailbox {
    mailbox_id: Uuid,
    mailbox_name: String,
    emails: Vec<ImapEmail>,
}

#[derive(Clone, Copy)]
enum MessageRefKind {
    Sequence,
    Uid,
}

#[derive(Clone, Copy)]
struct StoreMode {
    replace: bool,
    silent: bool,
}

impl<S: ImapStore, D: Detector> Session<S, D> {
    fn new(store: S, validator: Arc<Validator<D>>) -> Self {
        Self {
            store,
            validator,
            principal: None,
            selected: None,
        }
    }

    async fn handle_request<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        writer: &mut W,
        line: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let request = parse_request_line(line)?;
        let result = match request.command.as_str() {
            "CAPABILITY" => self.handle_capability(&request.tag, writer).await,
            "NOOP" => self.handle_noop(&request.tag, writer).await,
            "LOGOUT" => self.handle_logout(&request.tag, writer).await.map(|_| false),
            "LOGIN" => self.handle_login(&request.tag, &request.arguments, writer).await,
            "LIST" => self.handle_list(&request.tag, writer).await,
            "SELECT" => self.handle_select(&request.tag, &request.arguments, writer).await,
            "FETCH" => {
                self.handle_fetch(&request.tag, &request.arguments, writer, MessageRefKind::Sequence)
                    .await
            }
            "STORE" => {
                self.handle_store(&request.tag, &request.arguments, writer, MessageRefKind::Sequence)
                    .await
            }
            "SEARCH" => {
                self.handle_search(&request.tag, &request.arguments, writer, MessageRefKind::Sequence)
                    .await
            }
            "UID" => self
                .handle_uid(reader, writer, &request.tag, &request.arguments)
                .await,
            "APPEND" => self
                .handle_append(reader, writer, &request.tag, &request.arguments)
                .await,
            other => {
                writer
                    .write_all(format!("{} BAD unsupported command {}\r\n", request.tag, other).as_bytes())
                    .await?;
                writer.flush().await?;
                Ok(true)
            }
        };

        match result {
            Ok(keep_running) => Ok(keep_running),
            Err(error) => {
                writer
                    .write_all(format!("{} NO {}\r\n", request.tag, sanitize_imap_text(&error.to_string())).as_bytes())
                    .await?;
                writer.flush().await?;
                Ok(true)
            }
        }
    }

    async fn handle_capability<W>(&self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer
            .write_all(format!("* CAPABILITY {}\r\n{} OK CAPABILITY completed\r\n", CAPABILITIES, tag).as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_noop<W>(&self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer
            .write_all(format!("{tag} OK NOOP completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_logout<W>(&self, tag: &str, writer: &mut W) -> Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_all(b"* BYE LPE IMAP signing off\r\n").await?;
        writer
            .write_all(format!("{tag} OK LOGOUT completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(())
    }

    async fn handle_login<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        if self.principal.is_some() {
            bail!("already authenticated");
        }

        let tokens = tokenize(arguments)?;
        if tokens.len() != 2 {
            bail!("LOGIN expects username and password");
        }
        let username = tokens[0].clone();
        let password = tokens[1].clone();
        let login = self
            .store
            .fetch_account_login(&normalize_login_name(&username, None))
            .await?
            .ok_or_else(|| anyhow!("invalid credentials"))?;
        if login.status != "active" || !verify_password(&login.password_hash, &password) {
            bail!("invalid credentials");
        }

        self.principal = Some(AccountPrincipal {
            account_id: login.account_id,
            email: login.email,
            display_name: login.display_name,
        });
        self.selected = None;

        writer
            .write_all(format!("{tag} OK LOGIN completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_list<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailboxes = self.store.ensure_imap_mailboxes(principal.account_id).await?;
        for mailbox in mailboxes {
            writer
                .write_all(
                    format!(
                        "* LIST () \"/\" \"{}\"\r\n",
                        sanitize_imap_quoted(&mailbox.name)
                    )
                    .as_bytes(),
                )
                .await?;
        }
        writer
            .write_all(format!("{tag} OK LIST completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_select<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailbox_name = tokenize(arguments)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("SELECT expects a mailbox name"))?;
        let mailboxes = self.store.ensure_imap_mailboxes(principal.account_id).await?;
        let mailbox = mailboxes
            .into_iter()
            .find(|candidate| mailbox_name_matches(&candidate.name, &candidate.role, &mailbox_name))
            .ok_or_else(|| anyhow!("mailbox not found"))?;
        let emails = self
            .store
            .fetch_imap_emails(principal.account_id, mailbox.id)
            .await?;
        let exists = emails.len();
        let unseen = emails.iter().filter(|email| email.unread).count();
        let uid_next = emails
            .last()
            .map(|email| email.uid.saturating_add(1))
            .unwrap_or(1);
        self.selected = Some(SelectedMailbox {
            mailbox_id: mailbox.id,
            mailbox_name: mailbox.name.clone(),
            emails,
        });

        writer.write_all(b"* FLAGS (\\Seen \\Flagged \\Draft)\r\n").await?;
        writer
            .write_all(format!("* {} EXISTS\r\n", exists).as_bytes())
            .await?;
        writer.write_all(b"* 0 RECENT\r\n").await?;
        writer
            .write_all(format!("* OK [UNSEEN {}] first unseen\r\n", if unseen == 0 { 0 } else { 1 }).as_bytes())
            .await?;
        writer
            .write_all(format!("* OK [UIDVALIDITY {}] stable uid validity\r\n", UID_VALIDITY).as_bytes())
            .await?;
        writer
            .write_all(format!("* OK [UIDNEXT {}] next uid\r\n", uid_next).as_bytes())
            .await?;
        writer
            .write_all(format!("{tag} OK [READ-WRITE] SELECT completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_fetch<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        ref_kind: MessageRefKind,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let (set_token, attr_token) = split_two(arguments)?;
        let requested = parse_fetch_attributes(attr_token)?;
        let selected = self.require_selected()?;
        let indices = resolve_message_indexes(&selected.emails, set_token, ref_kind)?;
        let mut mark_seen_ids = Vec::new();

        for index in indices {
            let email = &selected.emails[index];
            let response = render_fetch_response(index + 1, email, &requested)?;
            writer.write_all(&response).await?;
            if requested.mark_seen && email.unread {
                mark_seen_ids.push(email.id);
            }
        }
        writer
            .write_all(format!("{tag} OK FETCH completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;

        if !mark_seen_ids.is_empty() {
            let principal = self.require_auth()?;
            let mailbox_id = selected.mailbox_id;
            self.store
                .update_imap_flags(principal.account_id, mailbox_id, &mark_seen_ids, Some(false), None)
                .await?;
            self.refresh_selected().await?;
        }

        Ok(true)
    }

    async fn handle_store<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        ref_kind: MessageRefKind,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let (set_token, rest) = split_two(arguments)?;
        let (mode_token, flags_token) = split_two(rest)?;
        let mode = parse_store_mode(mode_token)?;
        let flags = parse_flag_list(flags_token)?;
        let selected = self.require_selected()?;
        let indices = resolve_message_indexes(&selected.emails, set_token, ref_kind)?;
        let ids = indices
            .iter()
            .map(|index| selected.emails[*index].id)
            .collect::<Vec<_>>();
        let unread = match mode.replace {
            true => Some(!flags.contains("\\Seen")),
            false if mode_token.starts_with("+") && flags.contains("\\Seen") => Some(false),
            false if mode_token.starts_with("-") && flags.contains("\\Seen") => Some(true),
            _ => None,
        };
        let flagged = match mode.replace {
            true => Some(flags.contains("\\Flagged")),
            false if mode_token.starts_with("+") && flags.contains("\\Flagged") => Some(true),
            false if mode_token.starts_with("-") && flags.contains("\\Flagged") => Some(false),
            _ => None,
        };

        let principal = self.require_auth()?;
        self.store
            .update_imap_flags(principal.account_id, selected.mailbox_id, &ids, unread, flagged)
            .await?;
        self.refresh_selected().await?;

        if !mode.silent {
            let selected = self.require_selected()?;
            for index in indices {
                let email = &selected.emails[index];
                writer
                    .write_all(
                        format!(
                            "* {} FETCH (FLAGS ({}))\r\n",
                            index + 1,
                            render_flags(email, &selected.mailbox_name)
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
        }

        writer
            .write_all(format!("{tag} OK STORE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_search<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        ref_kind: MessageRefKind,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        let selected = self.require_selected()?;
        let criteria = SearchCriteria::from_tokens(&tokens)?;
        let text_ids = if let Some(text) = criteria.text.as_deref() {
            let principal = self.require_auth()?;
            Some(
                self.store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(selected.mailbox_id),
                        Some(text),
                        0,
                        selected.emails.len() as u64 + 1,
                    )
                    .await?
                    .ids
                    .into_iter()
                    .collect::<HashSet<_>>(),
            )
        } else {
            None
        };

        let mut matches = Vec::new();
        for (index, email) in selected.emails.iter().enumerate() {
            if !criteria.matches(email, text_ids.as_ref()) {
                continue;
            }
            matches.push(match ref_kind {
                MessageRefKind::Sequence => (index + 1).to_string(),
                MessageRefKind::Uid => email.uid.to_string(),
            });
        }

        writer
            .write_all(format!("* SEARCH {}\r\n", matches.join(" ")).as_bytes())
            .await?;
        writer
            .write_all(format!("{tag} OK SEARCH completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_uid<R, W>(
        &mut self,
        _reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
        arguments: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let (command, rest) = split_two(arguments)?;
        match command.to_ascii_uppercase().as_str() {
            "FETCH" => self
                .handle_fetch(tag, rest, writer, MessageRefKind::Uid)
                .await,
            "STORE" => self
                .handle_store(tag, rest, writer, MessageRefKind::Uid)
                .await,
            "SEARCH" => self
                .handle_search(tag, rest, writer, MessageRefKind::Uid)
                .await,
            other => bail!("UID {} is not supported", other),
        }
    }

    async fn handle_append<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
        arguments: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        if tokens.len() < 2 {
            bail!("APPEND expects mailbox and literal size");
        }
        let mailbox_name = &tokens[0];
        if !mailbox_name_matches("Drafts", "drafts", mailbox_name) {
            bail!("APPEND is only allowed for Drafts");
        }
        let literal_size = parse_literal_size(tokens.last().unwrap())?;

        writer.write_all(b"+ Ready for literal data\r\n").await?;
        writer.flush().await?;

        let mut literal = vec![0u8; literal_size];
        reader.read_exact(&mut literal).await?;
        let mut line_end = [0u8; 2];
        reader.read_exact(&mut line_end).await?;

        validate_append_attachments(&self.validator, &literal)?;
        let parsed = parse_rfc822_message(&literal)?;
        let principal = self.require_auth()?;
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|address| address.display_name.clone())
            .or_else(|| Some(principal.display_name.clone()));
        let from_address = parsed
            .from
            .map(|address| address.email)
            .unwrap_or_else(|| principal.email.clone());

        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: principal.account_id,
                    source: "imap-append".to_string(),
                    from_display,
                    from_address,
                    to: parsed
                        .to
                        .into_iter()
                        .map(|recipient| lpe_storage::SubmittedRecipientInput {
                            address: recipient.email,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    cc: parsed
                        .cc
                        .into_iter()
                        .map(|recipient| lpe_storage::SubmittedRecipientInput {
                            address: recipient.email,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    bcc: Vec::new(),
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.message_id,
                    mime_blob_ref: Some(format!("imap-append:{}", Uuid::new_v4())),
                    size_octets: literal.len() as i64,
                    attachments: parsed.attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-append".to_string(),
                    subject: "draft message append".to_string(),
                },
            )
            .await?;

        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_name.eq_ignore_ascii_case("Drafts")) {
            self.refresh_selected().await?;
        }

        writer
            .write_all(format!("{tag} OK APPEND completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    fn require_auth(&self) -> Result<&AccountPrincipal> {
        self.principal
            .as_ref()
            .ok_or_else(|| anyhow!("authentication required"))
    }

    fn require_selected(&self) -> Result<&SelectedMailbox> {
        self.selected
            .as_ref()
            .ok_or_else(|| anyhow!("SELECT a mailbox first"))
    }

    async fn refresh_selected(&mut self) -> Result<()> {
        let Some(selected) = self.selected.as_ref() else {
            return Ok(());
        };
        let principal = self.require_auth()?;
        self.selected = Some(SelectedMailbox {
            mailbox_id: selected.mailbox_id,
            mailbox_name: selected.mailbox_name.clone(),
            emails: self
                .store
                .fetch_imap_emails(principal.account_id, selected.mailbox_id)
                .await?,
        });
        Ok(())
    }
}

#[derive(Debug)]
struct RequestLine {
    tag: String,
    command: String,
    arguments: String,
}

#[derive(Default)]
struct SearchCriteria {
    all: bool,
    seen: Option<bool>,
    flagged: Option<bool>,
    text: Option<String>,
}

impl SearchCriteria {
    fn from_tokens(tokens: &[String]) -> Result<Self> {
        if tokens.is_empty() {
            return Ok(Self {
                all: true,
                ..Self::default()
            });
        }

        let mut criteria = Self::default();
        let mut cursor = 0usize;
        while cursor < tokens.len() {
            match tokens[cursor].to_ascii_uppercase().as_str() {
                "ALL" => criteria.all = true,
                "SEEN" => criteria.seen = Some(true),
                "UNSEEN" => criteria.seen = Some(false),
                "FLAGGED" => criteria.flagged = Some(true),
                "UNFLAGGED" => criteria.flagged = Some(false),
                "TEXT" | "SUBJECT" | "FROM" | "TO" => {
                    let value = tokens
                        .get(cursor + 1)
                        .ok_or_else(|| anyhow!("SEARCH {} requires an argument", tokens[cursor]))?;
                    criteria.text = Some(value.clone());
                    cursor += 1;
                }
                other => bail!("unsupported SEARCH criterion {}", other),
            }
            cursor += 1;
        }

        Ok(criteria)
    }

    fn matches(&self, email: &ImapEmail, text_ids: Option<&HashSet<Uuid>>) -> bool {
        if let Some(seen) = self.seen {
            if seen == email.unread {
                return false;
            }
        }
        if let Some(flagged) = self.flagged {
            if flagged != email.flagged {
                return false;
            }
        }
        if let Some(ids) = text_ids {
            if !ids.contains(&email.id) {
                return false;
            }
        }
        true
    }
}

struct FetchAttributes {
    items: Vec<String>,
    mark_seen: bool,
}

fn parse_request_line(line: &str) -> Result<RequestLine> {
    let mut parts = line.splitn(3, ' ');
    let tag = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing command tag"))?;
    let command = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing command"))?;
    Ok(RequestLine {
        tag: tag.to_string(),
        command: command.to_ascii_uppercase(),
        arguments: parts.next().unwrap_or_default().trim().to_string(),
    })
}

fn tokenize(input: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut cursor = 0usize;

    while cursor < chars.len() {
        while cursor < chars.len() && chars[cursor].is_whitespace() {
            cursor += 1;
        }
        if cursor >= chars.len() {
            break;
        }

        match chars[cursor] {
            '"' => {
                cursor += 1;
                let mut token = String::new();
                while cursor < chars.len() {
                    match chars[cursor] {
                        '"' => {
                            cursor += 1;
                            break;
                        }
                        '\\' if cursor + 1 < chars.len() => {
                            token.push(chars[cursor + 1]);
                            cursor += 2;
                        }
                        ch => {
                            token.push(ch);
                            cursor += 1;
                        }
                    }
                }
                tokens.push(token);
            }
            '(' => {
                let start = cursor;
                let mut depth = 0usize;
                while cursor < chars.len() {
                    match chars[cursor] {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                cursor += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    cursor += 1;
                }
                tokens.push(chars[start..cursor].iter().collect());
            }
            _ => {
                let start = cursor;
                while cursor < chars.len() && !chars[cursor].is_whitespace() {
                    cursor += 1;
                }
                tokens.push(chars[start..cursor].iter().collect());
            }
        }
    }

    Ok(tokens)
}

fn split_two(input: &str) -> Result<(&str, &str)> {
    let trimmed = input.trim();
    let Some(index) = trimmed.find(char::is_whitespace) else {
        bail!("invalid command syntax");
    };
    Ok((&trimmed[..index], trimmed[index..].trim()))
}

fn mailbox_name_matches(display_name: &str, role: &str, requested: &str) -> bool {
    requested.eq_ignore_ascii_case(display_name)
        || (role == "inbox" && requested.eq_ignore_ascii_case("INBOX"))
}

fn parse_fetch_attributes(input: &str) -> Result<FetchAttributes> {
    let upper = input.trim().to_ascii_uppercase();
    let expanded = match upper.as_str() {
        "ALL" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
            "UID".to_string(),
        ],
        "FAST" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
        ],
        "FULL" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
            "BODY[]".to_string(),
            "UID".to_string(),
        ],
        _ => {
            let source = input.trim().trim_start_matches('(').trim_end_matches(')');
            source
                .split_whitespace()
                .map(|item| item.to_ascii_uppercase())
                .collect()
        }
    };
    if expanded.is_empty() {
        bail!("FETCH expects at least one attribute");
    }
    let mark_seen = expanded.iter().any(|item| {
        matches!(
            item.as_str(),
            "BODY[]" | "BODY[TEXT]" | "RFC822" | "RFC822.TEXT"
        )
    });
    Ok(FetchAttributes {
        items: expanded,
        mark_seen,
    })
}

fn render_fetch_response(sequence: usize, email: &ImapEmail, requested: &FetchAttributes) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(format!("* {} FETCH (", sequence).as_bytes());
    let mut first = true;
    for item in &requested.items {
        if !first {
            output.extend_from_slice(b" ");
        }
        first = false;
        match item.as_str() {
            "UID" => output.extend_from_slice(format!("UID {}", email.uid).as_bytes()),
            "FLAGS" => output.extend_from_slice(
                format!("FLAGS ({})", render_flags(email, &email.mailbox_name)).as_bytes(),
            ),
            "INTERNALDATE" => output.extend_from_slice(
                format!("INTERNALDATE \"{}\"", format_internal_date(email)).as_bytes(),
            ),
            "RFC822.SIZE" => {
                output.extend_from_slice(format!("RFC822.SIZE {}", email.size_octets.max(0)).as_bytes())
            }
            "BODY[HEADER]" | "BODY.PEEK[HEADER]" => {
                append_literal(&mut output, item, render_header(email).as_bytes());
            }
            "BODY[TEXT]" | "BODY.PEEK[TEXT]" | "RFC822.TEXT" => {
                append_literal(&mut output, item, email.body_text.as_bytes());
            }
            "BODY[]" | "BODY.PEEK[]" | "RFC822" => {
                append_literal(&mut output, item, render_full_message(email).as_bytes());
            }
            other => bail!("unsupported FETCH attribute {}", other),
        }
    }
    output.extend_from_slice(b")\r\n");
    Ok(output)
}

fn append_literal(output: &mut Vec<u8>, label: &str, value: &[u8]) {
    output.extend_from_slice(format!("{} {{{}}}\r\n", label, value.len()).as_bytes());
    output.extend_from_slice(value);
}

fn render_flags(email: &ImapEmail, mailbox_name: &str) -> String {
    let mut flags = Vec::new();
    if !email.unread {
        flags.push("\\Seen");
    }
    if email.flagged {
        flags.push("\\Flagged");
    }
    if mailbox_name.eq_ignore_ascii_case("Drafts") {
        flags.push("\\Draft");
    }
    flags.join(" ")
}

fn render_header(email: &ImapEmail) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Date: {}", email.sent_at.as_deref().unwrap_or(&email.received_at)));
    lines.push(format!("From: {}", render_address_header(email.from_display.as_deref(), &email.from_address)));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_recipient_header(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_recipient_header(&email.cc)));
    }
    if !email.bcc.is_empty() && matches!(email.mailbox_role.as_str(), "drafts" | "sent") {
        lines.push(format!("Bcc: {}", render_recipient_header(&email.bcc)));
    }
    lines.push(format!("Subject: {}", email.subject));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", message_id));
    }
    lines.join("\r\n") + "\r\n\r\n"
}

fn render_full_message(email: &ImapEmail) -> String {
    format!("{}{}", render_header(email), email.body_text)
}

fn render_recipient_header(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| render_address_header(recipient.display_name.as_deref(), &recipient.address))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_address_header(display_name: Option<&str>, address: &str) -> String {
    match display_name.map(str::trim).filter(|value| !value.is_empty()) {
        Some(display) => format!("{} <{}>", display, address),
        None => address.to_string(),
    }
}

fn format_internal_date(email: &ImapEmail) -> String {
    let source = email.sent_at.as_deref().unwrap_or(&email.received_at);
    let month = match &source[5..7] {
        "01" => "Jan",
        "02" => "Feb",
        "03" => "Mar",
        "04" => "Apr",
        "05" => "May",
        "06" => "Jun",
        "07" => "Jul",
        "08" => "Aug",
        "09" => "Sep",
        "10" => "Oct",
        "11" => "Nov",
        "12" => "Dec",
        _ => "Jan",
    };
    format!(
        "{}-{}-{} {} +0000",
        &source[8..10],
        month,
        &source[0..4],
        &source[11..19]
    )
}

fn resolve_message_indexes(
    emails: &[ImapEmail],
    set_token: &str,
    ref_kind: MessageRefKind,
) -> Result<Vec<usize>> {
    let max_sequence = emails.len() as u32;
    let mut indexes = Vec::new();
    for segment in set_token.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        if let Some((start, end)) = segment.split_once(':') {
            let start = resolve_set_value(start, emails, max_sequence, ref_kind)?;
            let end = resolve_set_value(end, emails, max_sequence, ref_kind)?;
            let (from, to) = if start <= end { (start, end) } else { (end, start) };
            for value in from..=to {
                if let Some(index) = find_message_index(emails, value, ref_kind) {
                    if !indexes.contains(&index) {
                        indexes.push(index);
                    }
                }
            }
        } else {
            let value = resolve_set_value(segment, emails, max_sequence, ref_kind)?;
            if let Some(index) = find_message_index(emails, value, ref_kind) {
                if !indexes.contains(&index) {
                    indexes.push(index);
                }
            }
        }
    }
    Ok(indexes)
}

fn resolve_set_value(
    token: &str,
    emails: &[ImapEmail],
    max_sequence: u32,
    ref_kind: MessageRefKind,
) -> Result<u32> {
    if token == "*" {
        return Ok(match ref_kind {
            MessageRefKind::Sequence => max_sequence,
            MessageRefKind::Uid => emails.last().map(|email| email.uid).unwrap_or(0),
        });
    }
    token.parse::<u32>().map_err(Into::into)
}

fn find_message_index(emails: &[ImapEmail], value: u32, ref_kind: MessageRefKind) -> Option<usize> {
    match ref_kind {
        MessageRefKind::Sequence => value.checked_sub(1).map(|index| index as usize).filter(|index| *index < emails.len()),
        MessageRefKind::Uid => emails.iter().position(|email| email.uid == value),
    }
}

fn parse_store_mode(token: &str) -> Result<StoreMode> {
    Ok(match token.to_ascii_uppercase().as_str() {
        "FLAGS" => StoreMode {
            replace: true,
            silent: false,
        },
        "FLAGS.SILENT" => StoreMode {
            replace: true,
            silent: true,
        },
        "+FLAGS" => StoreMode {
            replace: false,
            silent: false,
        },
        "+FLAGS.SILENT" => StoreMode {
            replace: false,
            silent: true,
        },
        "-FLAGS" => StoreMode {
            replace: false,
            silent: false,
        },
        "-FLAGS.SILENT" => StoreMode {
            replace: false,
            silent: true,
        },
        other => bail!("unsupported STORE mode {}", other),
    })
}

fn parse_flag_list(token: &str) -> Result<HashSet<String>> {
    let source = token
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let mut flags = HashSet::new();
    for item in source.split_whitespace() {
        flags.insert(item.to_string());
    }
    Ok(flags)
}

fn parse_literal_size(token: &str) -> Result<usize> {
    let value = token
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');
    value.parse::<usize>().map_err(Into::into)
}

fn sanitize_imap_text(value: &str) -> String {
    value.replace('\r', " ").replace('\n', " ")
}

fn sanitize_imap_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn validate_append_attachments<D: Detector>(validator: &Validator<D>, bytes: &[u8]) -> Result<()> {
    for attachment in collect_mime_attachment_parts(bytes)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::ImapAppend,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "IMAP APPEND blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

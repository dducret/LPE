use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{
    authenticate_bearer_access_token, authenticate_plain_credentials, AccountPrincipal,
};
use lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, ImapEmail, JmapEmailAddress, JmapMailbox,
    SubmitMessageInput,
};
use std::{collections::HashSet, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};
use uuid::Uuid;

mod store;

use crate::store::ImapStore;

const CAPABILITIES: &str = "IMAP4rev1 AUTH=XOAUTH2 SASL-IR IDLE MOVE NAMESPACE UIDPLUS CONDSTORE";
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

        write_half.write_all(b"* OK LPE IMAP ready\r\n").await?;
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
    mailbox_role: String,
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

#[derive(Clone, Copy)]
struct StoreCondstore {
    unchanged_since: Option<u64>,
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
            "LOGOUT" => self
                .handle_logout(&request.tag, writer)
                .await
                .map(|_| false),
            "LOGIN" => {
                self.handle_login(&request.tag, &request.arguments, writer)
                    .await
            }
            "AUTHENTICATE" => {
                self.handle_authenticate(&request.tag, &request.arguments, writer)
                    .await
            }
            "LIST" => self.handle_list(&request.tag, writer).await,
            "NAMESPACE" => self.handle_namespace(&request.tag, writer).await,
            "STATUS" => {
                self.handle_status(&request.tag, &request.arguments, writer)
                    .await
            }
            "CREATE" => {
                self.handle_create(&request.tag, &request.arguments, writer)
                    .await
            }
            "DELETE" => {
                self.handle_delete(&request.tag, &request.arguments, writer)
                    .await
            }
            "RENAME" => {
                self.handle_rename(&request.tag, &request.arguments, writer)
                    .await
            }
            "SELECT" => {
                self.handle_select(&request.tag, &request.arguments, writer)
                    .await
            }
            "FETCH" => {
                self.handle_fetch(
                    &request.tag,
                    &request.arguments,
                    writer,
                    MessageRefKind::Sequence,
                )
                .await
            }
            "STORE" => {
                self.handle_store(
                    &request.tag,
                    &request.arguments,
                    writer,
                    MessageRefKind::Sequence,
                )
                .await
            }
            "SEARCH" => {
                self.handle_search(
                    &request.tag,
                    &request.arguments,
                    writer,
                    MessageRefKind::Sequence,
                )
                .await
            }
            "COPY" => {
                self.handle_copy(
                    &request.tag,
                    &request.arguments,
                    writer,
                    MessageRefKind::Sequence,
                )
                .await
            }
            "MOVE" => {
                self.handle_move(
                    &request.tag,
                    &request.arguments,
                    writer,
                    MessageRefKind::Sequence,
                )
                .await
            }
            "UID" => {
                self.handle_uid(reader, writer, &request.tag, &request.arguments)
                    .await
            }
            "IDLE" => self.handle_idle(reader, writer, &request.tag).await,
            "APPEND" => {
                self.handle_append(reader, writer, &request.tag, &request.arguments)
                    .await
            }
            other => {
                writer
                    .write_all(
                        format!("{} BAD unsupported command {}\r\n", request.tag, other).as_bytes(),
                    )
                    .await?;
                writer.flush().await?;
                Ok(true)
            }
        };

        match result {
            Ok(keep_running) => Ok(keep_running),
            Err(error) => {
                writer
                    .write_all(
                        format!(
                            "{} NO {}\r\n",
                            request.tag,
                            sanitize_imap_text(&error.to_string())
                        )
                        .as_bytes(),
                    )
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
            .write_all(
                format!(
                    "* CAPABILITY {}\r\n{} OK CAPABILITY completed\r\n",
                    CAPABILITIES, tag
                )
                .as_bytes(),
            )
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
        self.principal = Some(
            authenticate_plain_credentials(&self.store, None, &username, &password, "imap").await?,
        );
        self.selected = None;

        writer
            .write_all(format!("{tag} OK LOGIN completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_authenticate<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        if self.principal.is_some() {
            bail!("already authenticated");
        }

        let tokens = tokenize(arguments)?;
        if tokens.len() != 2 {
            bail!("AUTHENTICATE expects mechanism and an initial response");
        }
        if !tokens[0].eq_ignore_ascii_case("XOAUTH2") {
            bail!("only AUTHENTICATE XOAUTH2 is supported");
        }
        let (username, bearer_token) = parse_xoauth2_initial_response(&tokens[1])?;
        self.principal = Some(
            authenticate_bearer_access_token(&self.store, Some(&username), &bearer_token, "imap")
                .await?,
        );
        self.selected = None;

        writer
            .write_all(format!("{tag} OK AUTHENTICATE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_list<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        for mailbox in mailboxes {
            writer
                .write_all(
                    format!(
                        "* LIST {} \"/\" \"{}\"\r\n",
                        render_list_flags(&mailbox.role),
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

    async fn handle_namespace<W>(&self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_auth()?;
        writer
            .write_all(
                format!("* NAMESPACE ((\"\" \"/\")) NIL NIL\r\n{tag} OK NAMESPACE completed\r\n")
                    .as_bytes(),
            )
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_status<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox = self.resolve_mailbox_by_name(arguments).await?;
        let principal = self.require_auth()?;
        let emails = self
            .store
            .fetch_imap_emails(principal.account_id, mailbox.id)
            .await?;
        let highest_modseq = self
            .store
            .fetch_imap_highest_modseq(principal.account_id)
            .await?;
        let requested = parse_status_items(arguments)?;
        let response = render_status_response(&mailbox, &emails, &requested, highest_modseq);

        writer.write_all(response.as_bytes()).await?;
        writer
            .write_all(format!("{tag} OK STATUS completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_create<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox_name = first_token(arguments, "CREATE expects a mailbox name")?;
        validate_flat_mailbox_name(&mailbox_name)?;
        let principal = self.require_auth()?;
        self.store
            .create_imap_mailbox(
                principal.account_id,
                &mailbox_name,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-create-mailbox".to_string(),
                    subject: format!("create mailbox {}", mailbox_name),
                },
            )
            .await?;

        writer
            .write_all(format!("{tag} OK CREATE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_delete<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox = self.resolve_mailbox_by_name(arguments).await?;
        let principal = self.require_auth()?;
        self.store
            .delete_imap_mailbox(
                principal.account_id,
                mailbox.id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-delete-mailbox".to_string(),
                    subject: format!("delete mailbox {}", mailbox.name),
                },
            )
            .await?;
        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_id == mailbox.id) {
            self.selected = None;
        }

        writer
            .write_all(format!("{tag} OK DELETE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_rename<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        if tokens.len() != 2 {
            bail!("RENAME expects source and target mailbox names");
        }
        let mailbox = self.resolve_mailbox_by_name(&tokens[0]).await?;
        validate_flat_mailbox_name(&tokens[1])?;
        let principal = self.require_auth()?;
        self.store
            .rename_imap_mailbox(
                principal.account_id,
                mailbox.id,
                &tokens[1],
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-rename-mailbox".to_string(),
                    subject: format!("rename mailbox {} to {}", mailbox.name, tokens[1]),
                },
            )
            .await?;
        if let Some(selected) = self.selected.as_mut() {
            if selected.mailbox_id == mailbox.id {
                selected.mailbox_name = tokens[1].clone();
            }
        }

        writer
            .write_all(format!("{tag} OK RENAME completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_select<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailbox_name = tokenize(arguments)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("SELECT expects a mailbox name"))?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mailbox = mailboxes
            .into_iter()
            .find(|candidate| mailbox_name_matches(&candidate.name, &candidate.role, &mailbox_name))
            .ok_or_else(|| anyhow!("mailbox not found"))?;
        let emails = self
            .store
            .fetch_imap_emails(principal.account_id, mailbox.id)
            .await?;
        let highest_modseq = self
            .store
            .fetch_imap_highest_modseq(principal.account_id)
            .await?;
        let exists = emails.len();
        let uid_next = emails
            .last()
            .map(|email| email.uid.saturating_add(1))
            .unwrap_or(1);
        self.selected = Some(SelectedMailbox {
            mailbox_id: mailbox.id,
            mailbox_name: mailbox.name.clone(),
            mailbox_role: mailbox.role.clone(),
            emails,
        });

        writer
            .write_all(b"* FLAGS (\\Seen \\Flagged \\Draft)\r\n")
            .await?;
        writer
            .write_all(format!("* {} EXISTS\r\n", exists).as_bytes())
            .await?;
        writer.write_all(b"* 0 RECENT\r\n").await?;
        writer
            .write_all(
                format!(
                    "* OK [UNSEEN {}] first unseen\r\n",
                    first_unseen_sequence(self.require_selected()?)
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(
                format!(
                    "* OK [UIDVALIDITY {}] stable uid validity\r\n",
                    UID_VALIDITY
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("* OK [UIDNEXT {}] next uid\r\n", uid_next).as_bytes())
            .await?;
        writer
            .write_all(
                format!("* OK [HIGHESTMODSEQ {}] highest modseq\r\n", highest_modseq).as_bytes(),
            )
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
                .update_imap_flags(
                    principal.account_id,
                    mailbox_id,
                    &mark_seen_ids,
                    Some(false),
                    None,
                    None,
                )
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
        let (set_token, condstore, mode_token, flags_token) = parse_store_arguments(arguments)?;
        let mode = parse_store_mode(&mode_token)?;
        let flags = parse_flag_list(&flags_token)?;
        let selected = self.require_selected()?;
        let indices = resolve_message_indexes(&selected.emails, &set_token, ref_kind)?;
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
        let modified_ids = self
            .store
            .update_imap_flags(
                principal.account_id,
                selected.mailbox_id,
                &ids,
                unread,
                flagged,
                condstore.unchanged_since,
            )
            .await?;
        self.refresh_selected().await?;

        if !mode.silent {
            let selected = self.require_selected()?;
            for index in indices {
                let email = &selected.emails[index];
                if modified_ids.contains(&email.id) {
                    continue;
                }
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

        let selected = self.require_selected()?;
        if modified_ids.is_empty() {
            writer
                .write_all(format!("{tag} OK STORE completed\r\n").as_bytes())
                .await?;
        } else {
            let modified_set = render_modified_set(selected, &modified_ids, ref_kind);
            writer
                .write_all(
                    format!(
                        "{tag} NO [MODIFIED {}] conditional STORE failed\r\n",
                        modified_set
                    )
                    .as_bytes(),
                )
                .await?;
        }
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
        let criteria = SearchExpression::from_tokens(&tokens)?;

        let mut matches = Vec::new();
        for (index, email) in selected.emails.iter().enumerate() {
            if !criteria.matches(email, index, &selected.emails, ref_kind)? {
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

    async fn handle_copy<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        ref_kind: MessageRefKind,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let (set_token, mailbox_token) = split_two(arguments)?;
        let target_mailbox = self.resolve_mailbox_by_name(mailbox_token).await?;
        let selected = self.require_selected()?;
        ensure_copy_allowed(&selected.mailbox_role, &target_mailbox.role)?;
        let indices = resolve_message_indexes(&selected.emails, set_token, ref_kind)?;
        let principal = self.require_auth()?;
        let mut source_uids = Vec::new();
        let mut target_uids = Vec::new();

        for index in indices {
            let email = &selected.emails[index];
            let copied = self
                .store
                .copy_imap_email(
                    principal.account_id,
                    email.id,
                    target_mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "imap-copy".to_string(),
                        subject: format!(
                            "copy message {} to mailbox {}",
                            email.id, target_mailbox.name
                        ),
                    },
                )
                .await?;
            source_uids.push(email.uid.to_string());
            target_uids.push(copied.uid.to_string());
        }

        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_id == target_mailbox.id)
        {
            self.refresh_selected().await?;
        }

        let response = if source_uids.is_empty() {
            format!("{tag} OK COPY completed\r\n")
        } else {
            format!(
                "{tag} OK [COPYUID {} {} {}] COPY completed\r\n",
                UID_VALIDITY,
                source_uids.join(","),
                target_uids.join(",")
            )
        };
        writer.write_all(response.as_bytes()).await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_move<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        ref_kind: MessageRefKind,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let (set_token, mailbox_token) = split_two(arguments)?;
        let target_mailbox = self.resolve_mailbox_by_name(mailbox_token).await?;
        let selected = self.require_selected()?.clone();
        ensure_move_allowed(&selected, &target_mailbox)?;
        let indices = resolve_message_indexes(&selected.emails, set_token, ref_kind)?;
        let principal = self.require_auth()?;
        let mut source_uids = Vec::new();
        let mut target_uids = Vec::new();

        for index in &indices {
            let email = &selected.emails[*index];
            let moved = self
                .store
                .move_imap_email(
                    principal.account_id,
                    email.id,
                    target_mailbox.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "imap-move".to_string(),
                        subject: format!(
                            "move message {} to mailbox {}",
                            email.id, target_mailbox.name
                        ),
                    },
                )
                .await?;
            source_uids.push(email.uid.to_string());
            target_uids.push(moved.uid.to_string());
        }

        self.refresh_selected().await?;
        for index in indices.iter().rev() {
            writer
                .write_all(format!("* {} EXPUNGE\r\n", index + 1).as_bytes())
                .await?;
        }
        writer
            .write_all(format!("* {} EXISTS\r\n", self.require_selected()?.emails.len()).as_bytes())
            .await?;

        let response = if source_uids.is_empty() {
            format!("{tag} OK MOVE completed\r\n")
        } else {
            format!(
                "{tag} OK [COPYUID {} {} {}] MOVE completed\r\n",
                UID_VALIDITY,
                source_uids.join(","),
                target_uids.join(",")
            )
        };
        writer.write_all(response.as_bytes()).await?;
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
            "FETCH" => {
                self.handle_fetch(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "STORE" => {
                self.handle_store(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "SEARCH" => {
                self.handle_search(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "COPY" => {
                self.handle_copy(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "MOVE" => {
                self.handle_move(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            other => bail!("UID {} is not supported", other),
        }
    }

    async fn handle_idle<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        self.require_auth()?;
        let mut previous = self.require_selected()?.clone();
        writer.write_all(b"+ idling\r\n").await?;
        writer.flush().await?;

        loop {
            let mut line = String::new();
            match timeout(Duration::from_secs(1), reader.read_line(&mut line)).await {
                Ok(Ok(0)) => return Ok(false),
                Ok(Ok(_)) => {
                    if line
                        .trim_end_matches(['\r', '\n'])
                        .eq_ignore_ascii_case("DONE")
                    {
                        break;
                    }
                    bail!("IDLE expects DONE to terminate");
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    self.refresh_selected().await?;
                    let current = self.require_selected()?.clone();
                    let updates = render_selected_updates(&previous, &current)?;
                    if !updates.is_empty() {
                        writer.write_all(updates.as_bytes()).await?;
                        writer.flush().await?;
                    }
                    previous = current;
                }
            }
        }

        writer
            .write_all(format!("{tag} OK IDLE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
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

        let saved = self
            .store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: principal.account_id,
                    submitted_by_account_id: principal.account_id,
                    source: "imap-append".to_string(),
                    from_display,
                    from_address,
                    sender_display: None,
                    sender_address: None,
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
                    unread: Some(false),
                    flagged: Some(false),
                    attachments: parsed.attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-append".to_string(),
                    subject: "draft message append".to_string(),
                },
            )
            .await?;

        let appended = self
            .store
            .fetch_imap_emails(principal.account_id, saved.draft_mailbox_id)
            .await?
            .into_iter()
            .find(|email| email.id == saved.message_id)
            .ok_or_else(|| anyhow!("saved draft message not found"))?;

        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_name.eq_ignore_ascii_case("Drafts"))
        {
            self.refresh_selected().await?;
        }

        writer
            .write_all(
                format!(
                    "{tag} OK [APPENDUID {} {}] APPEND completed\r\n",
                    UID_VALIDITY, appended.uid
                )
                .as_bytes(),
            )
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
            mailbox_role: selected.mailbox_role.clone(),
            emails: self
                .store
                .fetch_imap_emails(principal.account_id, selected.mailbox_id)
                .await?,
        });
        Ok(())
    }

    async fn resolve_mailbox_by_name(&self, arguments: &str) -> Result<JmapMailbox> {
        let mailbox_name = first_token(arguments, "mailbox name is required")?;
        let principal = self.require_auth()?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        mailboxes
            .into_iter()
            .find(|candidate| mailbox_name_matches(&candidate.name, &candidate.role, &mailbox_name))
            .ok_or_else(|| anyhow!("mailbox not found"))
    }
}

#[derive(Debug)]
struct RequestLine {
    tag: String,
    command: String,
    arguments: String,
}

enum SearchExpression {
    All,
    Seen(bool),
    Flagged(bool),
    Text(String),
    Subject(String),
    From(String),
    To(String),
    Cc(String),
    Body(String),
    Header(String, String),
    Before(i32),
    On(i32),
    Since(i32),
    Larger(i64),
    Smaller(i64),
    MessageSet(String, MessageRefKind),
    Not(Box<SearchExpression>),
    Or(Box<SearchExpression>, Box<SearchExpression>),
    And(Vec<SearchExpression>),
}

impl SearchExpression {
    fn from_tokens(tokens: &[String]) -> Result<Self> {
        if tokens.is_empty() {
            return Ok(Self::All);
        }

        let mut cursor = 0usize;
        let mut expressions = Vec::new();
        while cursor < tokens.len() {
            expressions.push(parse_search_key(tokens, &mut cursor)?);
        }

        if expressions.len() == 1 {
            Ok(expressions.pop().unwrap())
        } else {
            Ok(Self::And(expressions))
        }
    }

    fn matches(
        &self,
        email: &ImapEmail,
        index: usize,
        emails: &[ImapEmail],
        ref_kind: MessageRefKind,
    ) -> Result<bool> {
        Ok(match self {
            Self::All => true,
            Self::Seen(seen) => (*seen) != email.unread,
            Self::Flagged(flagged) => *flagged == email.flagged,
            Self::Text(value) => search_email_text(email).contains(&normalize_search_text(value)),
            Self::Subject(value) => {
                normalize_search_text(&email.subject).contains(&normalize_search_text(value))
            }
            Self::From(value) => searchable_sender(email).contains(&normalize_search_text(value)),
            Self::To(value) => {
                searchable_recipients(&email.to).contains(&normalize_search_text(value))
            }
            Self::Cc(value) => {
                searchable_recipients(&email.cc).contains(&normalize_search_text(value))
            }
            Self::Body(value) => {
                normalize_search_text(&email.body_text).contains(&normalize_search_text(value))
            }
            Self::Header(name, value) => {
                searchable_header_value(email, name).contains(&normalize_search_text(value))
            }
            Self::Before(value) => message_search_date(email)? < *value,
            Self::On(value) => message_search_date(email)? == *value,
            Self::Since(value) => message_search_date(email)? >= *value,
            Self::Larger(value) => email.size_octets > *value,
            Self::Smaller(value) => email.size_octets < *value,
            Self::MessageSet(set, criterion_kind) => {
                let evaluation_kind = match criterion_kind {
                    MessageRefKind::Uid => MessageRefKind::Uid,
                    MessageRefKind::Sequence => ref_kind,
                };
                message_matches_set(email, index, emails, set, evaluation_kind)?
            }
            Self::Not(expression) => !expression.matches(email, index, emails, ref_kind)?,
            Self::Or(left, right) => {
                left.matches(email, index, emails, ref_kind)?
                    || right.matches(email, index, emails, ref_kind)?
            }
            Self::And(expressions) => expressions.iter().all(|expression| {
                expression
                    .matches(email, index, emails, ref_kind)
                    .unwrap_or(false)
            }),
        })
    }
}

struct FetchAttributes {
    items: Vec<String>,
    mark_seen: bool,
}

fn parse_store_arguments(input: &str) -> Result<(String, StoreCondstore, String, String)> {
    let tokens = tokenize(input)?;
    if tokens.len() < 3 {
        bail!("STORE expects a message set, mode, and flag list");
    }

    let set_token = tokens[0].clone();
    let mut cursor = 1usize;
    let mut condstore = StoreCondstore {
        unchanged_since: None,
    };
    if tokens[cursor]
        .to_ascii_uppercase()
        .starts_with("(UNCHANGEDSINCE ")
    {
        condstore = parse_store_condstore(&tokens[cursor])?;
        cursor += 1;
    }
    if tokens.len() != cursor + 2 {
        bail!("STORE expects a message set, mode, and flag list");
    }

    Ok((
        set_token,
        condstore,
        tokens[cursor].clone(),
        tokens[cursor + 1].clone(),
    ))
}

fn parse_store_condstore(token: &str) -> Result<StoreCondstore> {
    let source = token
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let parts = source.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 || !parts[0].eq_ignore_ascii_case("UNCHANGEDSINCE") {
        bail!("unsupported STORE modifier {}", token);
    }
    Ok(StoreCondstore {
        unchanged_since: Some(parts[1].parse::<u64>()?),
    })
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

fn render_list_flags(role: &str) -> &'static str {
    match role {
        "inbox" => "(\\Inbox)",
        "sent" => "(\\Sent)",
        "drafts" => "(\\Drafts)",
        _ => "()",
    }
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

fn render_fetch_response(
    sequence: usize,
    email: &ImapEmail,
    requested: &FetchAttributes,
) -> Result<Vec<u8>> {
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
            "MODSEQ" => output.extend_from_slice(format!("MODSEQ ({})", email.modseq).as_bytes()),
            "INTERNALDATE" => output.extend_from_slice(
                format!("INTERNALDATE \"{}\"", format_internal_date(email)).as_bytes(),
            ),
            "RFC822.SIZE" => output
                .extend_from_slice(format!("RFC822.SIZE {}", email.size_octets.max(0)).as_bytes()),
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

fn render_status_response(
    mailbox: &JmapMailbox,
    emails: &[ImapEmail],
    requested: &[String],
    highest_modseq: u64,
) -> String {
    let uid_next = emails
        .last()
        .map(|email| email.uid.saturating_add(1))
        .unwrap_or(1);
    let unseen = emails.iter().filter(|email| email.unread).count();
    let items = requested
        .iter()
        .map(|item| match item.as_str() {
            "MESSAGES" => format!("MESSAGES {}", emails.len()),
            "RECENT" => "RECENT 0".to_string(),
            "UIDNEXT" => format!("UIDNEXT {}", uid_next),
            "UIDVALIDITY" => format!("UIDVALIDITY {}", UID_VALIDITY),
            "UNSEEN" => format!("UNSEEN {}", unseen),
            "HIGHESTMODSEQ" => format!("HIGHESTMODSEQ {}", highest_modseq),
            _ => format!("{} 0", item),
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "* STATUS \"{}\" ({})\r\n",
        sanitize_imap_quoted(&mailbox.name),
        items
    )
}

fn render_header(email: &ImapEmail) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        email.sent_at.as_deref().unwrap_or(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_address_header(email.from_display.as_deref(), &email.from_address)
    ));
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

fn render_visible_header(email: &ImapEmail) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        email.sent_at.as_deref().unwrap_or(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_address_header(email.from_display.as_deref(), &email.from_address)
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_recipient_header(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_recipient_header(&email.cc)));
    }
    lines.push(format!("Subject: {}", email.subject));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", message_id));
    }
    lines.join("\r\n")
}

fn render_recipient_header(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| {
            render_address_header(recipient.display_name.as_deref(), &recipient.address)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_address_header(display_name: Option<&str>, address: &str) -> String {
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
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
            let (from, to) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
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
        MessageRefKind::Sequence => value
            .checked_sub(1)
            .map(|index| index as usize)
            .filter(|index| *index < emails.len()),
        MessageRefKind::Uid => emails.iter().position(|email| email.uid == value),
    }
}

fn message_matches_set(
    email: &ImapEmail,
    index: usize,
    emails: &[ImapEmail],
    set_token: &str,
    ref_kind: MessageRefKind,
) -> Result<bool> {
    let max_value = match ref_kind {
        MessageRefKind::Sequence => emails.len() as u32,
        MessageRefKind::Uid => emails.last().map(|candidate| candidate.uid).unwrap_or(0),
    };
    let value = match ref_kind {
        MessageRefKind::Sequence => (index + 1) as u32,
        MessageRefKind::Uid => email.uid,
    };

    for segment in set_token.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        if let Some((start, end)) = segment.split_once(':') {
            let start = resolve_set_value(start, emails, max_value, ref_kind)?;
            let end = resolve_set_value(end, emails, max_value, ref_kind)?;
            let (from, to) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            if value >= from && value <= to {
                return Ok(true);
            }
        } else if value == resolve_set_value(segment, emails, max_value, ref_kind)? {
            return Ok(true);
        }
    }

    Ok(false)
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

fn parse_status_items(arguments: &str) -> Result<Vec<String>> {
    let tokens = tokenize(arguments)?;
    if tokens.len() < 2 {
        bail!("STATUS expects a mailbox name and item list");
    }
    let source = tokens[1..].join(" ");
    let requested = source
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split_whitespace()
        .map(|item| item.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if requested.is_empty() {
        bail!("STATUS expects at least one data item");
    }
    for item in &requested {
        if !matches!(
            item.as_str(),
            "MESSAGES" | "RECENT" | "UIDNEXT" | "UIDVALIDITY" | "UNSEEN" | "HIGHESTMODSEQ"
        ) {
            bail!("unsupported STATUS item {}", item);
        }
    }
    Ok(requested)
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

fn render_modified_set(
    selected: &SelectedMailbox,
    modified_ids: &[Uuid],
    ref_kind: MessageRefKind,
) -> String {
    let mut values = Vec::new();
    for (index, email) in selected.emails.iter().enumerate() {
        if !modified_ids.contains(&email.id) {
            continue;
        }
        values.push(match ref_kind {
            MessageRefKind::Sequence => (index + 1).to_string(),
            MessageRefKind::Uid => email.uid.to_string(),
        });
    }
    values.join(",")
}

fn parse_literal_size(token: &str) -> Result<usize> {
    let value = token.trim().trim_start_matches('{').trim_end_matches('}');
    value.parse::<usize>().map_err(Into::into)
}

fn parse_search_key(tokens: &[String], cursor: &mut usize) -> Result<SearchExpression> {
    let token = tokens
        .get(*cursor)
        .ok_or_else(|| anyhow!("unexpected end of SEARCH criteria"))?
        .clone();
    *cursor += 1;

    if token.starts_with('(') && token.ends_with(')') && token.len() >= 2 {
        return SearchExpression::from_tokens(&tokenize(&token[1..token.len() - 1])?);
    }

    Ok(match token.to_ascii_uppercase().as_str() {
        "ALL" => SearchExpression::All,
        "SEEN" => SearchExpression::Seen(true),
        "UNSEEN" => SearchExpression::Seen(false),
        "FLAGGED" => SearchExpression::Flagged(true),
        "UNFLAGGED" => SearchExpression::Flagged(false),
        "TEXT" => SearchExpression::Text(next_search_argument(tokens, cursor, "TEXT")?),
        "SUBJECT" => SearchExpression::Subject(next_search_argument(tokens, cursor, "SUBJECT")?),
        "FROM" => SearchExpression::From(next_search_argument(tokens, cursor, "FROM")?),
        "TO" => SearchExpression::To(next_search_argument(tokens, cursor, "TO")?),
        "CC" => SearchExpression::Cc(next_search_argument(tokens, cursor, "CC")?),
        "BODY" => SearchExpression::Body(next_search_argument(tokens, cursor, "BODY")?),
        "HEADER" => SearchExpression::Header(
            next_search_argument(tokens, cursor, "HEADER")?,
            next_search_argument(tokens, cursor, "HEADER")?,
        ),
        "BEFORE" => SearchExpression::Before(parse_search_date(&next_search_argument(
            tokens, cursor, "BEFORE",
        )?)?),
        "ON" => SearchExpression::On(parse_search_date(&next_search_argument(
            tokens, cursor, "ON",
        )?)?),
        "SINCE" => SearchExpression::Since(parse_search_date(&next_search_argument(
            tokens, cursor, "SINCE",
        )?)?),
        "LARGER" => SearchExpression::Larger(
            next_search_argument(tokens, cursor, "LARGER")?.parse::<i64>()?,
        ),
        "SMALLER" => SearchExpression::Smaller(
            next_search_argument(tokens, cursor, "SMALLER")?.parse::<i64>()?,
        ),
        "UID" => SearchExpression::MessageSet(
            next_search_argument(tokens, cursor, "UID")?,
            MessageRefKind::Uid,
        ),
        "NOT" => SearchExpression::Not(Box::new(parse_search_key(tokens, cursor)?)),
        "OR" => SearchExpression::Or(
            Box::new(parse_search_key(tokens, cursor)?),
            Box::new(parse_search_key(tokens, cursor)?),
        ),
        other if looks_like_message_set(other) => {
            SearchExpression::MessageSet(token, MessageRefKind::Sequence)
        }
        other => bail!("unsupported SEARCH criterion {}", other),
    })
}

fn next_search_argument(tokens: &[String], cursor: &mut usize, criterion: &str) -> Result<String> {
    let value = tokens
        .get(*cursor)
        .cloned()
        .ok_or_else(|| anyhow!("SEARCH {} requires an argument", criterion))?;
    *cursor += 1;
    Ok(value)
}

fn looks_like_message_set(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '*' | ':' | ','))
}

fn normalize_search_text(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn searchable_sender(email: &ImapEmail) -> String {
    normalize_search_text(&render_address_header(
        email.from_display.as_deref(),
        &email.from_address,
    ))
}

fn searchable_recipients(recipients: &[JmapEmailAddress]) -> String {
    normalize_search_text(&render_recipient_header(recipients))
}

fn search_email_text(email: &ImapEmail) -> String {
    normalize_search_text(&format!(
        "{}\n{}\n{}",
        render_visible_header(email),
        email.body_text,
        email.preview
    ))
}

fn searchable_header_value(email: &ImapEmail, name: &str) -> String {
    match name.trim().to_ascii_uppercase().as_str() {
        "FROM" => searchable_sender(email),
        "TO" => searchable_recipients(&email.to),
        "CC" => searchable_recipients(&email.cc),
        "BCC" => String::new(),
        "SUBJECT" => normalize_search_text(&email.subject),
        "DATE" => normalize_search_text(email.sent_at.as_deref().unwrap_or(&email.received_at)),
        "MESSAGE-ID" => {
            normalize_search_text(email.internet_message_id.as_deref().unwrap_or_default())
        }
        _ => search_email_text(email),
    }
}

fn parse_search_date(value: &str) -> Result<i32> {
    let parts = value.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("invalid SEARCH date {}", value);
    }
    let day = parts[0].trim().parse::<i32>()?;
    let month = match parts[1].trim().to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => bail!("invalid SEARCH month {}", value),
    };
    let year = parts[2].trim().parse::<i32>()?;
    Ok((year * 10_000) + (month * 100) + day)
}

fn message_search_date(email: &ImapEmail) -> Result<i32> {
    let source = email.sent_at.as_deref().unwrap_or(&email.received_at);
    if source.len() < 10 {
        bail!("invalid message date");
    }
    let year = source[0..4].parse::<i32>()?;
    let month = source[5..7].parse::<i32>()?;
    let day = source[8..10].parse::<i32>()?;
    Ok((year * 10_000) + (month * 100) + day)
}

fn first_token(arguments: &str, error: &str) -> Result<String> {
    tokenize(arguments)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!(error.to_string()))
}

fn validate_flat_mailbox_name(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("mailbox name is required");
    }
    if trimmed.contains('/') {
        bail!("hierarchical mailbox names are not supported yet");
    }
    Ok(())
}

fn ensure_copy_allowed(source_role: &str, target_role: &str) -> Result<()> {
    if matches!(source_role, "drafts" | "sent") || matches!(target_role, "drafts" | "sent") {
        bail!("COPY does not support Sent or Drafts because those states stay canonical");
    }
    Ok(())
}

fn ensure_move_allowed(selected: &SelectedMailbox, target_mailbox: &JmapMailbox) -> Result<()> {
    if selected.mailbox_id == target_mailbox.id {
        bail!("MOVE target mailbox must differ from the selected mailbox");
    }
    if matches!(selected.mailbox_role.as_str(), "drafts" | "sent")
        || matches!(target_mailbox.role.as_str(), "drafts" | "sent")
    {
        bail!("MOVE does not support Sent or Drafts because those states stay canonical");
    }
    Ok(())
}

fn render_selected_updates(
    previous: &SelectedMailbox,
    current: &SelectedMailbox,
) -> Result<String> {
    let mut output = String::new();

    let current_ids = current
        .emails
        .iter()
        .map(|email| email.id)
        .collect::<HashSet<_>>();
    let mut removed_sequences = previous
        .emails
        .iter()
        .enumerate()
        .filter_map(|(index, email)| (!current_ids.contains(&email.id)).then_some(index + 1))
        .collect::<Vec<_>>();
    removed_sequences.sort_unstable_by(|left, right| right.cmp(left));
    for sequence in removed_sequences {
        output.push_str(&format!("* {} EXPUNGE\r\n", sequence));
    }

    if previous.emails.len() != current.emails.len() {
        output.push_str(&format!("* {} EXISTS\r\n", current.emails.len()));
    }

    for (index, email) in current.emails.iter().enumerate() {
        let Some(previous_email) = previous
            .emails
            .iter()
            .find(|candidate| candidate.id == email.id)
        else {
            continue;
        };
        if previous_email.unread != email.unread || previous_email.flagged != email.flagged {
            output.push_str(&format!(
                "* {} FETCH (FLAGS ({}))\r\n",
                index + 1,
                render_flags(email, &current.mailbox_name)
            ));
        }
    }

    Ok(output)
}

fn first_unseen_sequence(selected: &SelectedMailbox) -> usize {
    selected
        .emails
        .iter()
        .position(|email| email.unread)
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn parse_xoauth2_initial_response(encoded: &str) -> Result<(String, String)> {
    let decoded = BASE64
        .decode(encoded.trim())
        .map_err(|_| anyhow!("invalid XOAUTH2 initial response"))?;
    let decoded = String::from_utf8(decoded).map_err(|_| anyhow!("invalid XOAUTH2 payload"))?;
    let mut username = None;
    let mut bearer_token = None;
    for segment in decoded.split('\u{1}') {
        if let Some(value) = segment.strip_prefix("user=") {
            let value = value.trim();
            if !value.is_empty() {
                username = Some(value.to_string());
            }
        } else if let Some(value) = segment.strip_prefix("auth=Bearer ") {
            let value = value.trim();
            if !value.is_empty() {
                bearer_token = Some(value.to_string());
            }
        }
    }
    Ok((
        username.ok_or_else(|| anyhow!("XOAUTH2 payload is missing the user field"))?,
        bearer_token.ok_or_else(|| anyhow!("XOAUTH2 payload is missing the bearer token"))?,
    ))
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

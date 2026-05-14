use anyhow::{anyhow, bail, Result};
use lpe_magika::{Detector, Validator};
use lpe_mail_auth::AccountPrincipal;
use lpe_storage::ImapEmail;
use std::{str, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    mailboxes::render_mailbox_path,
    parse::parse_request_line,
    render::{render_imap_mailbox_response_path, render_selected_updates, sanitize_imap_text},
    store::ImapStore,
};

const CAPABILITIES: &str =
    "IMAP4rev2 IMAP4rev1 AUTH=PLAIN AUTH=LOGIN AUTH=XOAUTH2 SASL-IR ID IDLE MOVE NAMESPACE UIDPLUS CONDSTORE ENABLE UTF8=ACCEPT ACL SPECIAL-USE UNSELECT";

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
            let mut raw_line = Vec::new();
            let bytes = reader.read_until(b'\n', &mut raw_line).await?;
            if bytes == 0 {
                break;
            }
            let line = match decode_command_line(&raw_line) {
                Ok(line) => line,
                Err(error) => {
                    let tag = command_tag_from_bytes(&raw_line).unwrap_or("*");
                    write_half
                        .write_all(format!("{tag} BAD {}\r\n", error).as_bytes())
                        .await?;
                    write_half.flush().await?;
                    continue;
                }
            };
            if line.is_empty() {
                continue;
            }
            let request = parse_request_line(&line)?;
            let line = if request.command == "APPEND" {
                line.to_string()
            } else {
                match read_command_literals(&mut reader, &mut write_half, &line).await {
                    Ok(line) => line,
                    Err(error) => {
                        write_half
                            .write_all(
                                format!(
                                    "{} BAD {}\r\n",
                                    request.tag,
                                    sanitize_imap_text(&error.to_string())
                                )
                                .as_bytes(),
                            )
                            .await?;
                        write_half.flush().await?;
                        continue;
                    }
                }
            };
            let keep_running = session
                .handle_request(&mut reader, &mut write_half, &line)
                .await?;
            if !keep_running {
                break;
            }
        }

        Ok(())
    }
}

async fn read_command_literals<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    initial_line: &str,
) -> Result<String>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut line = initial_line.to_string();
    while let Some((prefix, size, synchronizing)) = trailing_literal(&line)? {
        if synchronizing {
            writer.write_all(b"+ Ready for literal data\r\n").await?;
            writer.flush().await?;
        }

        let mut literal = vec![0u8; size];
        reader.read_exact(&mut literal).await?;

        let mut rest = Vec::new();
        reader.read_until(b'\n', &mut rest).await?;
        let rest = decode_command_line(&rest)?;

        line = format!("{prefix}\"{}\"", quote_literal_token(&literal)?);
        line.push_str(&rest);
    }
    Ok(line)
}

fn decode_command_line(bytes: &[u8]) -> Result<&str> {
    let trimmed = trim_line_end(bytes);
    str::from_utf8(trimmed).map_err(|_| anyhow!("invalid UTF-8 in command line"))
}

fn trim_line_end(mut bytes: &[u8]) -> &[u8] {
    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn command_tag_from_bytes(bytes: &[u8]) -> Option<&str> {
    let bytes = trim_line_end(bytes);
    let end = bytes
        .iter()
        .position(|byte| byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    if end == 0 {
        return None;
    }
    str::from_utf8(&bytes[..end]).ok()
}

fn trailing_literal(line: &str) -> Result<Option<(&str, usize, bool)>> {
    let Some(close_index) = line.strip_suffix('}').map(|_| line.len() - 1) else {
        return Ok(None);
    };
    let Some(open_index) = line[..close_index].rfind('{') else {
        return Ok(None);
    };
    let mut size_token = &line[open_index + 1..close_index];
    let synchronizing = !size_token.ends_with('+');
    if !synchronizing {
        size_token = &size_token[..size_token.len() - 1];
    }
    if size_token.is_empty()
        || !size_token
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return Ok(None);
    }
    let size = size_token.parse::<usize>()?;
    if size > 4096 {
        bail!("command literal is too large");
    }
    Ok(Some((&line[..open_index], size, synchronizing)))
}

fn quote_literal_token(bytes: &[u8]) -> Result<String> {
    Ok(str::from_utf8(bytes)
        .map_err(|_| anyhow!("invalid UTF-8 in command literal"))?
        .replace('\\', "\\\\")
        .replace('"', "\\\""))
}

pub async fn serve(listener: TcpListener, store: impl ImapStore) -> Result<()> {
    ImapServer::new(store).serve(listener).await
}

#[derive(Clone)]
pub(crate) struct Session<S, D> {
    pub(crate) store: S,
    pub(crate) validator: Arc<Validator<D>>,
    pub(crate) principal: Option<AccountPrincipal>,
    pub(crate) selected: Option<SelectedMailbox>,
    pub(crate) utf8_accept_enabled: bool,
}

#[derive(Clone)]
pub(crate) struct SelectedMailbox {
    pub(crate) mailbox_id: Uuid,
    pub(crate) mailbox_name: String,
    pub(crate) mailbox_role: String,
    pub(crate) emails: Vec<ImapEmail>,
    pub(crate) read_only: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum MessageRefKind {
    Sequence,
    Uid,
}

impl<S: ImapStore, D: Detector> Session<S, D> {
    pub(crate) fn new(store: S, validator: Arc<Validator<D>>) -> Self {
        Self {
            store,
            validator,
            principal: None,
            selected: None,
            utf8_accept_enabled: false,
        }
    }

    pub(crate) async fn handle_request<R, W>(
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
                self.handle_authenticate(&request.tag, &request.arguments, reader, writer)
                    .await
            }
            "AUTH" => {
                self.handle_authenticate(&request.tag, &request.arguments, reader, writer)
                    .await
            }
            "LIST" => {
                self.handle_list(&request.tag, &request.arguments, writer)
                    .await
            }
            "XLIST" => {
                self.handle_xlist(&request.tag, &request.arguments, writer)
                    .await
            }
            "LSUB" => {
                self.handle_lsub(&request.tag, &request.arguments, writer)
                    .await
            }
            "SUBSCRIBE" => {
                self.handle_subscribe(&request.tag, &request.arguments, writer)
                    .await
            }
            "UNSUBSCRIBE" => {
                self.handle_unsubscribe(&request.tag, &request.arguments, writer)
                    .await
            }
            "ID" => self.handle_id(&request.tag, writer).await,
            "NAMESPACE" => self.handle_namespace(&request.tag, writer).await,
            "ENABLE" => {
                self.handle_enable(&request.tag, &request.arguments, writer)
                    .await
            }
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
            "EXAMINE" => {
                self.handle_examine(&request.tag, &request.arguments, writer)
                    .await
            }
            "CHECK" => self.handle_check(&request.tag, writer).await,
            "CLOSE" => self.handle_close(&request.tag, writer).await,
            "UNSELECT" => self.handle_unselect(&request.tag, writer).await,
            "EXPUNGE" => self.handle_expunge(&request.tag, writer).await,
            "GETACL" => {
                self.handle_getacl(&request.tag, &request.arguments, writer)
                    .await
            }
            "GETQUOTAROOT" => {
                self.handle_getquotaroot(&request.tag, &request.arguments, writer)
                    .await
            }
            "GETQUOTA" => {
                self.handle_getquota(&request.tag, &request.arguments, writer)
                    .await
            }
            "MYRIGHTS" => {
                self.handle_myrights(&request.tag, &request.arguments, writer)
                    .await
            }
            "LISTRIGHTS" => {
                self.handle_listrights(&request.tag, &request.arguments, writer)
                    .await
            }
            "SETACL" => {
                self.handle_setacl(&request.tag, &request.arguments, writer)
                    .await
            }
            "DELETEACL" => {
                self.handle_deleteacl(&request.tag, &request.arguments, writer)
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
                warn!(
                    command = %other,
                    "unsupported IMAP command"
                );
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
                warn!(
                    command = %request.command,
                    error = %sanitize_imap_text(&error.to_string()),
                    "IMAP command failed"
                );
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

    async fn handle_noop<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.refresh_selected_updates(writer).await?;
        writer
            .write_all(format!("{tag} OK NOOP completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn refresh_selected_updates<W>(&mut self, writer: &mut W) -> Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let previous = self.selected.clone();
        self.refresh_selected().await?;
        let Some(previous) = previous.as_ref() else {
            return Ok(());
        };
        let Some(current) = self.selected.as_ref() else {
            return Ok(());
        };
        let updates = render_selected_updates(previous, current)?;
        if !updates.is_empty() {
            writer.write_all(updates.as_bytes()).await?;
        }
        Ok(())
    }

    async fn handle_id<W>(&self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer
            .write_all(
                format!("* ID (\"name\" \"LPE\" \"vendor\" \"LPE\")\r\n{tag} OK ID completed\r\n")
                    .as_bytes(),
            )
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_enable<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_auth()?;
        let enabled = arguments
            .split_whitespace()
            .filter_map(|capability| {
                if capability.eq_ignore_ascii_case("CONDSTORE") {
                    Some("CONDSTORE")
                } else if capability.eq_ignore_ascii_case("UTF8=ACCEPT") {
                    self.utf8_accept_enabled = true;
                    Some("UTF8=ACCEPT")
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let enabled_response = if enabled.is_empty() {
            "* ENABLED\r\n".to_string()
        } else {
            format!("* ENABLED {}\r\n", enabled.join(" "))
        };
        writer.write_all(enabled_response.as_bytes()).await?;
        writer
            .write_all(format!("{tag} OK ENABLE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_getquotaroot<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox = self.resolve_mailbox_by_name(arguments).await?;
        let principal = self.require_auth()?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mailbox_path = render_mailbox_path(&mailbox, &mailboxes);
        writer
            .write_all(
                format!(
                    "* QUOTAROOT {}\r\n",
                    render_imap_mailbox_response_path(&mailbox_path, self.utf8_accept_enabled)
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("{tag} OK GETQUOTAROOT completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn handle_getquota<W>(&self, tag: &str, _arguments: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_auth()?;
        writer
            .write_all(format!("{tag} OK GETQUOTA completed\r\n").as_bytes())
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

    pub(crate) fn require_auth(&self) -> Result<&AccountPrincipal> {
        self.principal
            .as_ref()
            .ok_or_else(|| anyhow!("authentication required"))
    }

    pub(crate) fn require_selected(&self) -> Result<&SelectedMailbox> {
        self.selected
            .as_ref()
            .ok_or_else(|| anyhow!("SELECT a mailbox first"))
    }

    pub(crate) async fn refresh_selected(&mut self) -> Result<()> {
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
            read_only: selected.read_only,
        });
        Ok(())
    }
}

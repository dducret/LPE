use anyhow::{anyhow, bail, Result};
use lpe_domain::MailboxNamePolicy;
use lpe_magika::Detector;
use lpe_storage::{AuditEntryInput, JmapMailbox};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::{
    parse::{parse_mailbox_path, parse_mailbox_path_token, tokenize},
    render::{
        first_unseen_sequence, mailbox_name_matches, parse_status_items,
        render_imap_mailbox_response_path, render_list_flags, render_status_response,
        resolve_message_indexes,
    },
    MessageRefKind, SelectedMailbox, Session,
};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_list<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_mailbox_listing(tag, arguments, writer, "LIST", false)
            .await
    }

    pub(crate) async fn handle_xlist<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_mailbox_listing(tag, arguments, writer, "XLIST", true)
            .await
    }

    async fn handle_mailbox_listing<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        command_name: &str,
        legacy_xlist: bool,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let pattern = parse_list_pattern(arguments)?;
        if pattern.is_empty() {
            writer
                .write_all(format!("* {command_name} (\\Noselect) \"/\" \"\"\r\n").as_bytes())
                .await?;
            writer
                .write_all(format!("{tag} OK {command_name} completed\r\n").as_bytes())
                .await?;
            writer.flush().await?;
            return Ok(true);
        }

        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mut matched = 0usize;
        for mailbox in &mailboxes {
            let mailbox_path = render_mailbox_path(mailbox, &mailboxes);
            if !mailbox_matches_pattern(mailbox, &mailbox_path, &pattern) {
                continue;
            }
            matched += 1;
            writer
                .write_all(
                    format!(
                        "* {} {} \"/\" {}\r\n",
                        command_name,
                        render_list_flags(&mailbox.role, legacy_xlist),
                        render_imap_mailbox_response_path(&mailbox_path, self.utf8_accept_enabled)
                    )
                    .as_bytes(),
                )
                .await?;
        }
        writer
            .write_all(format!("{tag} OK {command_name} completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        info!(
            command = %command_name,
            pattern = %pattern,
            matched,
            "IMAP mailbox listing completed"
        );
        Ok(true)
    }

    pub(crate) async fn handle_lsub<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let pattern = parse_list_pattern(arguments)?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mut matched = 0usize;
        for mailbox in &mailboxes {
            if !mailbox.is_subscribed {
                continue;
            }
            let mailbox_path = render_mailbox_path(mailbox, &mailboxes);
            if !mailbox_matches_pattern(mailbox, &mailbox_path, &pattern) {
                continue;
            }
            matched += 1;
            writer
                .write_all(
                    format!(
                        "* LSUB {} \"/\" {}\r\n",
                        render_list_flags(&mailbox.role, false),
                        render_imap_mailbox_response_path(&mailbox_path, self.utf8_accept_enabled)
                    )
                    .as_bytes(),
                )
                .await?;
        }
        writer
            .write_all(format!("{tag} OK LSUB completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        info!(pattern = %pattern, matched, "IMAP LSUB completed");
        Ok(true)
    }

    pub(crate) async fn handle_subscribe<W>(
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
        self.store
            .set_mailbox_subscription(
                principal.account_id,
                mailbox.id,
                true,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-subscribe-mailbox".to_string(),
                    subject: format!("subscribe mailbox {}", mailbox.name),
                },
            )
            .await?;
        writer
            .write_all(format!("{tag} OK SUBSCRIBE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_unsubscribe<W>(
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
        self.store
            .set_mailbox_subscription(
                principal.account_id,
                mailbox.id,
                false,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-unsubscribe-mailbox".to_string(),
                    subject: format!("unsubscribe mailbox {}", mailbox.name),
                },
            )
            .await?;
        writer
            .write_all(format!("{tag} OK UNSUBSCRIBE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_namespace<W>(&self, tag: &str, writer: &mut W) -> Result<bool>
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

    pub(crate) async fn handle_status<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mailbox_path = parse_mailbox_path_token(arguments, "mailbox name is required")?;
        let mailbox = mailboxes
            .iter()
            .find(|candidate| mailbox_matches_path(candidate, &mailboxes, &mailbox_path))
            .cloned()
            .ok_or_else(|| anyhow!("mailbox not found"))?;
        let rendered_path = render_mailbox_path(&mailbox, &mailboxes);
        let emails = self
            .store
            .fetch_imap_emails(principal.account_id, mailbox.id)
            .await?;
        let state = self
            .store
            .fetch_imap_mailbox_state(principal.account_id, mailbox.id)
            .await?;
        let requested = parse_status_items(arguments)?;
        let response = render_status_response(
            &rendered_path,
            &emails,
            &requested,
            &state,
            self.utf8_accept_enabled,
        );

        writer.write_all(response.as_bytes()).await?;
        writer
            .write_all(format!("{tag} OK STATUS completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        info!(
            mailbox = %rendered_path,
            mailbox_role = %mailbox.role,
            messages = emails.len(),
            highest_modseq = state.highest_modseq,
            requested = %requested.join(" "),
            "IMAP STATUS completed"
        );
        Ok(true)
    }

    pub(crate) async fn handle_create<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox_name =
            parse_mailbox_path_token(arguments, "CREATE expects a mailbox name")?.into_string();
        let principal = self.require_auth()?;
        self.store
            .create_imap_mailbox(
                principal.account_id,
                mailbox_name.as_str(),
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

    pub(crate) async fn handle_delete<W>(
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

    pub(crate) async fn handle_rename<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        if tokens.len() != 2 {
            bail!("RENAME expects source and target mailbox names");
        }
        let source_path = parse_mailbox_path(&tokens[0])?;
        let mailbox = self.resolve_mailbox_path(&source_path).await?;
        let target_name = parse_mailbox_path(&tokens[1])?.into_string();
        let principal = self.require_auth()?;
        let renamed = self
            .store
            .rename_imap_mailbox(
                principal.account_id,
                mailbox.id,
                target_name.as_str(),
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-rename-mailbox".to_string(),
                    subject: format!("rename mailbox {} to {}", mailbox.name, target_name),
                },
            )
            .await?;
        if let Some(selected) = self.selected.as_mut() {
            if selected.mailbox_id == mailbox.id {
                selected.mailbox_name = renamed.name;
            }
        }

        writer
            .write_all(format!("{tag} OK RENAME completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_select<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_select_mode(tag, arguments, writer, false).await
    }

    pub(crate) async fn handle_examine<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_select_mode(tag, arguments, writer, true).await
    }

    async fn handle_select_mode<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
        read_only: bool,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let principal = self.require_auth()?;
        let mailbox_path = parse_mailbox_path_token(arguments, "SELECT expects a mailbox name")?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        let mailbox = mailboxes
            .iter()
            .find(|candidate| mailbox_matches_path(candidate, &mailboxes, &mailbox_path))
            .cloned()
            .ok_or_else(|| anyhow!("mailbox not found"))?;
        let selected_mailbox_name = render_mailbox_path(&mailbox, &mailboxes);
        let emails = self
            .store
            .fetch_imap_emails(principal.account_id, mailbox.id)
            .await?;
        let state = self
            .store
            .fetch_imap_mailbox_state(principal.account_id, mailbox.id)
            .await?;
        let exists = emails.len();
        self.selected = Some(SelectedMailbox {
            mailbox_id: mailbox.id,
            mailbox_name: selected_mailbox_name.clone(),
            mailbox_role: mailbox.role.clone(),
            emails,
            read_only,
        });

        writer
            .write_all(b"* FLAGS (\\Seen \\Flagged \\Deleted \\Draft)\r\n")
            .await?;
        writer
            .write_all(b"* OK [PERMANENTFLAGS (\\Seen \\Flagged \\Deleted)] supported flags\r\n")
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
                    state.uid_validity
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("* OK [UIDNEXT {}] next uid\r\n", state.uid_next).as_bytes())
            .await?;
        writer
            .write_all(
                format!(
                    "* OK [HIGHESTMODSEQ {}] highest modseq\r\n",
                    state.highest_modseq
                )
                .as_bytes(),
            )
            .await?;
        let access = if read_only { "READ-ONLY" } else { "READ-WRITE" };
        let command_name = if read_only { "EXAMINE" } else { "SELECT" };
        writer
            .write_all(format!("{tag} OK [{access}] {command_name} completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        info!(
            command = %command_name,
            mailbox = %selected_mailbox_name,
            mailbox_role = %mailbox.role,
            exists,
            uid_next = state.uid_next,
            highest_modseq = state.highest_modseq,
            read_only,
            "IMAP mailbox selected"
        );
        Ok(true)
    }

    pub(crate) async fn handle_check<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_selected()?;
        self.refresh_selected_updates(writer).await?;
        writer
            .write_all(format!("{tag} OK CHECK completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_close<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let selected = self.require_selected()?.clone();
        if !selected.read_only {
            let indices = selected
                .emails
                .iter()
                .enumerate()
                .filter_map(|(index, email)| email.deleted.then_some(index))
                .collect::<Vec<_>>();
            if !indices.is_empty() {
                self.delete_selected_indices(&selected, &indices).await?;
            }
        }
        self.selected = None;
        writer
            .write_all(format!("{tag} OK CLOSE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_unselect<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_selected()?;
        self.selected = None;
        writer
            .write_all(format!("{tag} OK UNSELECT completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_expunge<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let selected = self.require_selected()?.clone();
        let indices = selected
            .emails
            .iter()
            .enumerate()
            .filter_map(|(index, email)| email.deleted.then_some(index))
            .collect::<Vec<_>>();
        self.expunge_selected_indices(&selected, &indices, writer)
            .await?;
        self.refresh_selected().await?;
        writer
            .write_all(format!("{tag} OK EXPUNGE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_uid_expunge<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let selected = self.require_selected()?.clone();
        let indices =
            resolve_message_indexes(&selected.emails, arguments.trim(), MessageRefKind::Uid)?
                .into_iter()
                .filter(|index| selected.emails[*index].deleted)
                .collect::<Vec<_>>();
        self.expunge_selected_indices(&selected, &indices, writer)
            .await?;
        self.refresh_selected().await?;
        writer
            .write_all(format!("{tag} OK UID EXPUNGE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn expunge_selected_indices<W>(
        &mut self,
        selected: &SelectedMailbox,
        indices: &[usize],
        writer: &mut W,
    ) -> Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        if indices.is_empty() {
            return Ok(());
        }
        self.delete_selected_indices(selected, indices).await?;
        for index in indices.iter().rev() {
            writer
                .write_all(format!("* {} EXPUNGE\r\n", index + 1).as_bytes())
                .await?;
        }
        Ok(())
    }

    async fn delete_selected_indices(
        &mut self,
        selected: &SelectedMailbox,
        indices: &[usize],
    ) -> Result<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let principal = self.require_auth()?;
        let ids = indices
            .iter()
            .map(|index| selected.emails[*index].id)
            .collect::<Vec<_>>();
        self.store
            .expunge_imap_deleted(
                principal.account_id,
                selected.mailbox_id,
                &ids,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-expunge".to_string(),
                    subject: format!(
                        "expunge {} messages from {}",
                        ids.len(),
                        selected.mailbox_name
                    ),
                },
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn resolve_mailbox_by_name(&self, arguments: &str) -> Result<JmapMailbox> {
        let mailbox_path = parse_mailbox_path_token(arguments, "mailbox name is required")?;
        self.resolve_mailbox_path(&mailbox_path).await
    }

    pub(crate) async fn resolve_mailbox_name(&self, mailbox_name: &str) -> Result<JmapMailbox> {
        let mailbox_path = parse_mailbox_path(mailbox_name)?;
        self.resolve_mailbox_path(&mailbox_path).await
    }

    async fn resolve_mailbox_path(
        &self,
        mailbox_path: &lpe_domain::MailboxPath,
    ) -> Result<JmapMailbox> {
        let principal = self.require_auth()?;
        let mailboxes = self
            .store
            .ensure_imap_mailboxes(principal.account_id)
            .await?;
        mailboxes
            .iter()
            .find(|candidate| mailbox_matches_path(candidate, &mailboxes, mailbox_path))
            .cloned()
            .ok_or_else(|| anyhow!("mailbox not found"))
    }
}

fn mailbox_matches_path(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    path: &lpe_domain::MailboxPath,
) -> bool {
    mailbox_name_matches(
        &render_mailbox_path(mailbox, mailboxes),
        &mailbox.role,
        path.as_str(),
    )
}

fn parse_list_pattern(arguments: &str) -> Result<String> {
    let tokens = tokenize(arguments)?;
    if tokens.len() < 2 {
        bail!("LIST expects reference name and mailbox pattern");
    }
    Ok(tokens[1].clone())
}

fn mailbox_pattern_matches(name: &str, pattern: &str) -> bool {
    MailboxNamePolicy::list_pattern_matches(name, pattern)
}

fn mailbox_matches_pattern(mailbox: &JmapMailbox, mailbox_path: &str, pattern: &str) -> bool {
    if mailbox_pattern_matches(mailbox_path, pattern) {
        return true;
    }

    special_mailbox_aliases(&mailbox.role)
        .iter()
        .any(|alias| mailbox_pattern_matches(alias, pattern))
}

pub(crate) fn render_mailbox_path(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> String {
    if mailbox.role == "inbox" {
        return "INBOX".to_string();
    }

    let mut segments = vec![mailbox.name.clone()];
    let mut parent_id = mailbox.parent_id;
    while let Some(id) = parent_id {
        let Some(parent) = mailboxes.iter().find(|candidate| candidate.id == id) else {
            break;
        };
        if parent.role == "inbox" {
            segments.push("INBOX".to_string());
        } else {
            segments.push(parent.name.clone());
        }
        parent_id = parent.parent_id;
    }
    segments.reverse();
    segments.join("/")
}

fn special_mailbox_aliases(role: &str) -> &'static [&'static str] {
    match role {
        "drafts" => &["Draft", "Drafts"],
        "sent" => &["Sent", "Sent Items", "Sent Messages"],
        "trash" => &["Deleted", "Deleted Items", "Trash"],
        "junk" => &["Junk", "Junk E-mail", "Junk Email", "Spam"],
        "archive" => &["Archive"],
        "outbox" => &["Outbox"],
        "rss_feeds" => &["RSS Feeds", "RSS Subscriptions"],
        "conversation_history" => &["Conversation History"],
        "sync_issues" => &["Sync Issues"],
        "conflicts" => &["Conflicts"],
        "local_failures" => &["Local Failures"],
        "server_failures" => &["Server Failures"],
        _ => &[],
    }
}

use anyhow::{anyhow, bail, Result};
use lpe_magika::Detector;
use lpe_storage::{AuditEntryInput, JmapMailbox};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::{
    parse::{first_token, tokenize},
    render::{
        first_unseen_sequence, mailbox_name_matches, parse_status_items, render_list_flags,
        render_mailbox_name, render_status_response, sanitize_imap_quoted,
    },
    SelectedMailbox, Session, UID_VALIDITY,
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
        for mailbox in mailboxes {
            let mailbox_name = render_mailbox_name(&mailbox);
            if !mailbox_pattern_matches(&mailbox_name, &pattern)
                && !mailbox_pattern_matches(&mailbox.name, &pattern)
            {
                continue;
            }
            matched += 1;
            writer
                .write_all(
                    format!(
                        "* {} {} \"/\" \"{}\"\r\n",
                        command_name,
                        render_list_flags(&mailbox.role, legacy_xlist),
                        sanitize_imap_quoted(&mailbox_name)
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
        for mailbox in mailboxes {
            let mailbox_name = render_mailbox_name(&mailbox);
            if !mailbox_pattern_matches(&mailbox_name, &pattern)
                && !mailbox_pattern_matches(&mailbox.name, &pattern)
            {
                continue;
            }
            matched += 1;
            writer
                .write_all(
                    format!(
                        "* LSUB {} \"/\" \"{}\"\r\n",
                        render_list_flags(&mailbox.role, false),
                        sanitize_imap_quoted(&mailbox_name)
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
        self.resolve_mailbox_by_name(arguments).await?;
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
        self.resolve_mailbox_by_name(arguments).await?;
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
        info!(
            mailbox = %render_mailbox_name(&mailbox),
            mailbox_role = %mailbox.role,
            messages = emails.len(),
            highest_modseq,
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
            read_only,
        });

        writer
            .write_all(b"* FLAGS (\\Seen \\Flagged \\Draft)\r\n")
            .await?;
        writer
            .write_all(b"* OK [PERMANENTFLAGS (\\Seen \\Flagged \\Draft)] supported flags\r\n")
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
        let access = if read_only { "READ-ONLY" } else { "READ-WRITE" };
        let command_name = if read_only { "EXAMINE" } else { "SELECT" };
        writer
            .write_all(format!("{tag} OK [{access}] {command_name} completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        info!(
            command = %command_name,
            mailbox = %render_mailbox_name(&mailbox),
            mailbox_role = %mailbox.role,
            exists,
            uid_next,
            highest_modseq,
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
        self.refresh_selected().await?;
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
        self.require_selected()?;
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
        self.require_selected()?;
        self.refresh_selected().await?;
        writer
            .write_all(format!("{tag} OK EXPUNGE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn resolve_mailbox_by_name(&self, arguments: &str) -> Result<JmapMailbox> {
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

fn parse_list_pattern(arguments: &str) -> Result<String> {
    let tokens = tokenize(arguments)?;
    if tokens.len() < 2 {
        bail!("LIST expects reference name and mailbox pattern");
    }
    Ok(tokens[1].clone())
}

fn mailbox_pattern_matches(name: &str, pattern: &str) -> bool {
    if pattern == "*" || pattern == "%" {
        return true;
    }
    wildcard_match(
        &name.to_ascii_uppercase(),
        &pattern.replace('%', "*").to_ascii_uppercase(),
    )
}

fn wildcard_match(value: &str, pattern: &str) -> bool {
    let value = value.as_bytes();
    let pattern = pattern.as_bytes();
    let (mut value_index, mut pattern_index) = (0usize, 0usize);
    let mut star_index = None;
    let mut star_value_index = 0usize;

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == value[value_index] || pattern[pattern_index] == b'?')
        {
            value_index += 1;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            star_value_index = value_index;
            pattern_index += 1;
        } else if let Some(index) = star_index {
            pattern_index = index + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

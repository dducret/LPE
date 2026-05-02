use anyhow::{anyhow, bail, Result};
use lpe_magika::Detector;
use lpe_storage::{AuditEntryInput, JmapMailbox};
use tokio::io::AsyncWriteExt;

use crate::{
    parse::{first_token, tokenize},
    render::{
        first_unseen_sequence, mailbox_name_matches, parse_status_items, render_list_flags,
        render_status_response, sanitize_imap_quoted,
    },
    SelectedMailbox, Session, UID_VALIDITY,
};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_list<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_mailbox_listing(tag, writer, "LIST").await
    }

    pub(crate) async fn handle_xlist<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.handle_mailbox_listing(tag, writer, "XLIST").await
    }

    async fn handle_mailbox_listing<W>(
        &mut self,
        tag: &str,
        writer: &mut W,
        command_name: &str,
    ) -> Result<bool>
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
                        "* {} {} \"/\" \"{}\"\r\n",
                        command_name,
                        render_list_flags(&mailbox.role),
                        sanitize_imap_quoted(&mailbox.name)
                    )
                    .as_bytes(),
                )
                .await?;
        }
        writer
            .write_all(format!("{tag} OK {command_name} completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_lsub<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
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
                        "* LSUB {} \"/\" \"{}\"\r\n",
                        render_list_flags(&mailbox.role),
                        sanitize_imap_quoted(&mailbox.name)
                    )
                    .as_bytes(),
                )
                .await?;
        }
        writer
            .write_all(format!("{tag} OK LSUB completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
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

use anyhow::{bail, Result};
use lpe_magika::Detector;
use lpe_storage::{AuditEntryInput, JmapMailbox};
use tokio::io::AsyncWriteExt;

use crate::{
    parse::{split_two, tokenize},
    render::{
        parse_fetch_attributes, render_fetch_response, render_flags, render_modified_set,
        resolve_message_indexes,
    },
    search::SearchExpression,
    store_args::{parse_flag_list, parse_store_arguments, parse_store_mode},
    MessageRefKind, SelectedMailbox, Session, UID_VALIDITY,
};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_fetch<W>(
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
            if requested.mark_seen && email.unread && !selected.read_only {
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

    pub(crate) async fn handle_store<W>(
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
        let selected = self.require_selected()?.clone();
        if selected.read_only {
            bail!("mailbox is read-only");
        }
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
            for message_id in &ids {
                let Some((index, email)) = selected
                    .emails
                    .iter()
                    .enumerate()
                    .find(|(_, email)| email.id == *message_id)
                else {
                    continue;
                };
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
            let modified_set = render_modified_set(&selected, &modified_ids, ref_kind);
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

    pub(crate) async fn handle_search<W>(
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

    pub(crate) async fn handle_copy<W>(
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

    pub(crate) async fn handle_move<W>(
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

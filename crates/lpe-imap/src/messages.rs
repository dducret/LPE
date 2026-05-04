use anyhow::{bail, Result};
use lpe_magika::Detector;
use lpe_storage::{AuditEntryInput, JmapMailbox};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::{
    parse::{split_two, tokenize},
    render::{
        ensure_uid_fetch_attributes, parse_fetch_attributes, render_fetch_response, render_flags,
        render_modified_set, resolve_message_indexes, FetchAttributes, FetchItem,
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
        let (set_token, attr_token, changed_since) = parse_fetch_arguments(arguments)?;
        let mut requested = parse_fetch_attributes(attr_token)?;
        if matches!(ref_kind, MessageRefKind::Uid) {
            ensure_uid_fetch_attributes(&mut requested);
        }
        ensure_condstore_fetch_attributes(&mut requested, changed_since);
        self.refresh_selected().await?;
        let selected = self.require_selected()?;
        let indices = resolve_message_indexes(&selected.emails, set_token, ref_kind)?
            .into_iter()
            .filter(|index| {
                changed_since
                    .map(|modseq| selected.emails[*index].modseq > modseq)
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let mut mark_seen_ids = Vec::new();
        let mut response_count = 0usize;
        let mut response_bytes = 0usize;
        let mut first_uid = None;
        let mut last_uid = None;

        for index in indices {
            let email = &selected.emails[index];
            let response = render_fetch_response(index + 1, email, &requested)?;
            response_count += 1;
            response_bytes += response.len();
            first_uid.get_or_insert(email.uid);
            last_uid = Some(email.uid);
            writer.write_all(&response).await?;
            if requested.mark_seen && email.unread && !selected.read_only {
                mark_seen_ids.push(email.id);
            }
        }
        writer
            .write_all(format!("{tag} OK FETCH completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;

        info!(
            mailbox = %selected.mailbox_name,
            mailbox_role = %selected.mailbox_role,
            ref_kind = %message_ref_kind_name(ref_kind),
            set = %set_token,
            attributes = %attr_token,
            changed_since = ?changed_since,
            total_messages = selected.emails.len(),
            responses = response_count,
            response_bytes,
            first_uid = ?first_uid,
            last_uid = ?last_uid,
            mark_seen = requested.mark_seen,
            marked_seen = mark_seen_ids.len(),
            "IMAP FETCH completed"
        );

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
        let deleted = match mode.replace {
            true => Some(flags.contains("\\Deleted")),
            false if mode_token.starts_with("+") && flags.contains("\\Deleted") => Some(true),
            false if mode_token.starts_with("-") && flags.contains("\\Deleted") => Some(false),
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
                deleted,
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
        let mut tokens = tokenize(arguments)?;
        strip_search_return_options(&mut tokens)?;
        if tokens
            .first()
            .is_some_and(|token| token.eq_ignore_ascii_case("CHARSET"))
        {
            if tokens.len() < 2 {
                bail!("SEARCH CHARSET requires a charset name");
            }
            tokens.drain(0..2);
        }
        self.refresh_selected().await?;
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
        info!(
            mailbox = %selected.mailbox_name,
            mailbox_role = %selected.mailbox_role,
            ref_kind = %message_ref_kind_name(ref_kind),
            criteria_count = tokens.len(),
            matches = matches.len(),
            total_messages = selected.emails.len(),
            "IMAP SEARCH completed"
        );
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

fn message_ref_kind_name(kind: MessageRefKind) -> &'static str {
    match kind {
        MessageRefKind::Sequence => "sequence",
        MessageRefKind::Uid => "uid",
    }
}

fn parse_fetch_arguments(arguments: &str) -> Result<(&str, &str, Option<u64>)> {
    let trimmed = arguments.trim();
    let (set_token, rest) = split_two(trimmed)?;
    let rest = rest.trim_start();
    if !rest.starts_with('(') {
        let (attr_token, modifier) = rest
            .split_once(char::is_whitespace)
            .map(|(attrs, modifier)| (attrs, modifier.trim()))
            .unwrap_or((rest, ""));
        return Ok((set_token, attr_token, parse_fetch_modifier(modifier)?));
    }

    let mut depth = 0usize;
    let mut attr_end = None;
    for (index, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    attr_end = Some(index + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }
    let attr_end = attr_end.ok_or_else(|| anyhow::anyhow!("invalid FETCH attribute list"))?;
    let attr_token = &rest[..attr_end];
    let modifier = rest[attr_end..].trim();
    Ok((set_token, attr_token, parse_fetch_modifier(modifier)?))
}

fn parse_fetch_modifier(modifier: &str) -> Result<Option<u64>> {
    if modifier.is_empty() {
        return Ok(None);
    }
    let inner = modifier
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| anyhow::anyhow!("unsupported FETCH modifier {}", modifier))?;
    let parts = inner.split_whitespace().collect::<Vec<_>>();
    if parts.len() == 2 && parts[0].eq_ignore_ascii_case("CHANGEDSINCE") {
        return Ok(Some(parts[1].parse()?));
    }
    bail!("unsupported FETCH modifier {}", modifier)
}

fn ensure_condstore_fetch_attributes(requested: &mut FetchAttributes, changed_since: Option<u64>) {
    if changed_since.is_some()
        && !requested
            .items
            .iter()
            .any(|item| matches!(item, FetchItem::Modseq))
    {
        requested.items.push(FetchItem::Modseq);
    }
}

fn strip_search_return_options(tokens: &mut Vec<String>) -> Result<()> {
    if !tokens
        .first()
        .is_some_and(|token| token.eq_ignore_ascii_case("RETURN"))
    {
        return Ok(());
    }
    if tokens.len() < 2 {
        bail!("SEARCH RETURN requires an option list");
    }
    tokens.drain(0..2);
    Ok(())
}

fn ensure_copy_allowed(source_role: &str, target_role: &str) -> Result<()> {
    if matches!(target_role, "drafts" | "sent") {
        bail!("COPY does not support Sent or Drafts as target mailboxes because those states stay canonical");
    }
    if source_role == "sent" {
        bail!("COPY does not support Sent as a source mailbox because Sent stays canonical");
    }
    if source_role == "drafts" && target_role != "trash" {
        bail!("COPY from Drafts is only supported to Trash for client deletion");
    }
    Ok(())
}

fn ensure_move_allowed(selected: &SelectedMailbox, target_mailbox: &JmapMailbox) -> Result<()> {
    if selected.mailbox_id == target_mailbox.id {
        bail!("MOVE target mailbox must differ from the selected mailbox");
    }
    if matches!(target_mailbox.role.as_str(), "drafts" | "sent") {
        bail!("MOVE does not support Sent or Drafts as target mailboxes because those states stay canonical");
    }
    if selected.mailbox_role == "sent" {
        bail!("MOVE does not support Sent as a source mailbox because Sent stays canonical");
    }
    if selected.mailbox_role == "drafts" && target_mailbox.role != "trash" {
        bail!("MOVE from Drafts is only supported to Trash for client deletion");
    }
    Ok(())
}

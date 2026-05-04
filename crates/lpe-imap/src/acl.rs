use anyhow::{bail, Result};
use lpe_magika::Detector;
use lpe_storage::{
    AuditEntryInput, MailboxDelegationGrantInput, SenderDelegationGrantInput, SenderDelegationRight,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio::io::AsyncWriteExt;

use crate::{
    parse::tokenize,
    render::{sanitize_imap_quoted, sanitize_imap_text},
    Session,
};

const MAILBOX_RIGHTS: &str = "lrswite";
const OWNER_RIGHTS: &str = "lrswiteapb";
const ASSIGNABLE_RIGHTS: &str = OWNER_RIGHTS;

#[derive(Clone, Copy)]
struct AclState {
    mailbox: bool,
    may_write: bool,
    send_as: bool,
    send_on_behalf: bool,
}

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_getacl<W>(
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
        let owner = self
            .store
            .fetch_account_identity(principal.account_id)
            .await?;
        let mailbox_grants = self
            .store
            .fetch_outgoing_mailbox_delegation_grants(principal.account_id)
            .await?;
        let sender_grants = self
            .store
            .fetch_outgoing_sender_delegation_grants(principal.account_id)
            .await?;

        let mut entries = vec![format!("{} {}", owner.email, OWNER_RIGHTS)];
        for (identifier, state) in combine_acl_state(&mailbox_grants, &sender_grants) {
            let rights = render_acl_rights(state, false);
            if rights.is_empty() {
                continue;
            }
            entries.push(format!("{} {}", identifier, rights));
        }

        writer
            .write_all(
                format!(
                    "* ACL \"{}\" {}\r\n",
                    sanitize_imap_quoted(&mailbox.name),
                    entries.join(" ")
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("{tag} OK GETACL completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_myrights<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mailbox = self.resolve_mailbox_by_name(arguments).await?;
        self.require_auth()?;

        writer
            .write_all(
                format!(
                    "* MYRIGHTS \"{}\" {}\r\n",
                    sanitize_imap_quoted(&mailbox.name),
                    OWNER_RIGHTS
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("{tag} OK MYRIGHTS completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_listrights<W>(
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
            bail!("LISTRIGHTS expects a mailbox name and identifier");
        }
        let mailbox = self.resolve_mailbox_by_name(&tokens[0]).await?;

        writer
            .write_all(
                format!(
                    "* LISTRIGHTS \"{}\" \"{}\" \"\" {}\r\n",
                    sanitize_imap_quoted(&mailbox.name),
                    sanitize_imap_quoted(&tokens[1]),
                    ASSIGNABLE_RIGHTS
                )
                .as_bytes(),
            )
            .await?;
        writer
            .write_all(format!("{tag} OK LISTRIGHTS completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_setacl<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        if tokens.len() != 3 {
            bail!("SETACL expects a mailbox name, identifier, and rights");
        }
        self.apply_acl_update(&tokens[0], &tokens[1], &tokens[2])
            .await?;

        writer
            .write_all(format!("{tag} OK SETACL completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_deleteacl<W>(
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
            bail!("DELETEACL expects a mailbox name and identifier");
        }
        self.apply_acl_update(&tokens[0], &tokens[1], "").await?;
        writer
            .write_all(format!("{tag} OK DELETEACL completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn apply_acl_update(
        &mut self,
        mailbox_name: &str,
        identifier: &str,
        rights: &str,
    ) -> Result<()> {
        let mailbox = self.resolve_mailbox_by_name(mailbox_name).await?;
        let principal = self.require_auth()?;
        let owner = self
            .store
            .fetch_account_identity(principal.account_id)
            .await?;
        let identifier = identifier.trim();
        if identifier.eq_ignore_ascii_case(&owner.email) {
            bail!("owner rights are fixed");
        }

        let mailbox_grants = self
            .store
            .fetch_outgoing_mailbox_delegation_grants(principal.account_id)
            .await?;
        let sender_grants = self
            .store
            .fetch_outgoing_sender_delegation_grants(principal.account_id)
            .await?;
        let mut current_grants = combine_acl_state(&mailbox_grants, &sender_grants);
        let current_state = current_grants.remove(&identifier.to_ascii_lowercase());
        let requested_state = parse_acl_state_update(current_state, rights)?;

        if (requested_state.send_as || requested_state.send_on_behalf) && !requested_state.mailbox {
            bail!("sender delegation rights require mailbox access rights");
        }

        let existing_mailbox = mailbox_grants
            .iter()
            .find(|grant| grant.grantee_email.eq_ignore_ascii_case(identifier))
            .map(|grant| grant.grantee_account_id);
        let existing_send_as = sender_grants
            .iter()
            .find(|grant| {
                grant.grantee_email.eq_ignore_ascii_case(identifier)
                    && grant.sender_right == "send_as"
            })
            .map(|grant| grant.grantee_account_id);
        let existing_send_on_behalf = sender_grants
            .iter()
            .find(|grant| {
                grant.grantee_email.eq_ignore_ascii_case(identifier)
                    && grant.sender_right == "send_on_behalf"
            })
            .map(|grant| grant.grantee_account_id);

        if requested_state.mailbox {
            let grant = self
                .store
                .upsert_mailbox_delegation_grant(
                    MailboxDelegationGrantInput {
                        owner_account_id: principal.account_id,
                        grantee_email: identifier.to_string(),
                        may_write: requested_state.may_write,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "imap-setacl-mailbox-grant".to_string(),
                        subject: format!("set acl {} {}", mailbox.name, identifier),
                    },
                )
                .await?;

            sync_sender_right(
                &self.store,
                principal,
                &mailbox.name,
                identifier,
                grant.grantee_account_id,
                SenderDelegationRight::SendAs,
                requested_state.send_as,
                existing_send_as.is_some(),
            )
            .await?;
            sync_sender_right(
                &self.store,
                principal,
                &mailbox.name,
                identifier,
                grant.grantee_account_id,
                SenderDelegationRight::SendOnBehalf,
                requested_state.send_on_behalf,
                existing_send_on_behalf.is_some(),
            )
            .await?;
        } else if let Some(grantee_account_id) = existing_send_as
            .or(existing_send_on_behalf)
            .or(existing_mailbox)
        {
            if existing_send_as.is_some() {
                self.store
                    .delete_sender_delegation_grant(
                        principal.account_id,
                        grantee_account_id,
                        SenderDelegationRight::SendAs,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "imap-setacl-delete-send-as".to_string(),
                            subject: format!("set acl {} {}", mailbox.name, identifier),
                        },
                    )
                    .await?;
            }
            if existing_send_on_behalf.is_some() {
                self.store
                    .delete_sender_delegation_grant(
                        principal.account_id,
                        grantee_account_id,
                        SenderDelegationRight::SendOnBehalf,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "imap-setacl-delete-send-on-behalf".to_string(),
                            subject: format!("set acl {} {}", mailbox.name, identifier),
                        },
                    )
                    .await?;
            }
            if existing_mailbox.is_some() {
                self.store
                    .delete_mailbox_delegation_grant(
                        principal.account_id,
                        grantee_account_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "imap-setacl-delete-mailbox-grant".to_string(),
                            subject: format!("set acl {} {}", mailbox.name, identifier),
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

fn combine_acl_state(
    mailbox_grants: &[lpe_storage::MailboxDelegationGrant],
    sender_grants: &[lpe_storage::SenderDelegationGrant],
) -> BTreeMap<String, AclState> {
    let mut grants = BTreeMap::new();
    for grant in mailbox_grants {
        let entry = grants
            .entry(grant.grantee_email.to_ascii_lowercase())
            .or_insert(AclState {
                mailbox: false,
                may_write: false,
                send_as: false,
                send_on_behalf: false,
            });
        entry.mailbox = true;
        entry.may_write = grant.may_write;
    }
    for grant in sender_grants {
        let entry = grants
            .entry(grant.grantee_email.to_ascii_lowercase())
            .or_insert(AclState {
                mailbox: false,
                may_write: false,
                send_as: false,
                send_on_behalf: false,
            });
        match grant.sender_right.as_str() {
            "send_as" => entry.send_as = true,
            "send_on_behalf" => entry.send_on_behalf = true,
            _ => {}
        }
    }
    grants
}

fn parse_acl_state_update(current: Option<AclState>, token: &str) -> Result<AclState> {
    let trimmed = token.trim_matches('"');
    if trimmed.is_empty() {
        return Ok(AclState {
            mailbox: false,
            may_write: false,
            send_as: false,
            send_on_behalf: false,
        });
    }

    let (mode, rights_source) = match trimmed.as_bytes().first().copied() {
        Some(b'+') => ('+', &trimmed[1..]),
        Some(b'-') => ('-', &trimmed[1..]),
        _ => ('=', trimmed),
    };
    let requested = parse_acl_rights(rights_source)?;
    let base = current.unwrap_or(AclState {
        mailbox: false,
        may_write: false,
        send_as: false,
        send_on_behalf: false,
    });

    Ok(match mode {
        '+' => AclState {
            mailbox: base.mailbox || requested.mailbox,
            may_write: base.may_write || requested.may_write,
            send_as: base.send_as || requested.send_as,
            send_on_behalf: base.send_on_behalf || requested.send_on_behalf,
        },
        '-' => AclState {
            mailbox: if requested.mailbox && !requested.may_write {
                false
            } else {
                base.mailbox
            },
            may_write: base.may_write && !requested.may_write,
            send_as: base.send_as && !requested.send_as,
            send_on_behalf: base.send_on_behalf && !requested.send_on_behalf,
        },
        _ => requested,
    })
}

fn parse_acl_rights(source: &str) -> Result<AclState> {
    let mut mailbox = false;
    let mut may_write = false;
    let mut send_as = false;
    let mut send_on_behalf = false;
    let mut unsupported = BTreeSet::new();

    for ch in source.chars() {
        match ch {
            'l' | 'r' => mailbox = true,
            's' | 'w' | 'i' | 't' | 'e' => {
                mailbox = true;
                may_write = true;
            }
            'p' => send_as = true,
            'b' => send_on_behalf = true,
            _ => {
                unsupported.insert(ch);
            }
        }
    }

    if !unsupported.is_empty() {
        bail!(
            "unsupported ACL rights {}",
            sanitize_imap_text(&unsupported.into_iter().collect::<String>())
        );
    }

    Ok(AclState {
        mailbox,
        may_write,
        send_as,
        send_on_behalf,
    })
}

fn render_acl_rights(state: AclState, owner: bool) -> String {
    let mut rights = String::new();
    if state.mailbox {
        if state.may_write {
            rights.push_str(MAILBOX_RIGHTS);
        } else {
            rights.push_str("lr");
        }
    }
    if owner {
        rights.push('a');
    }
    if state.send_as || owner {
        rights.push('p');
    }
    if state.send_on_behalf || owner {
        rights.push('b');
    }
    rights
}

async fn sync_sender_right<S: crate::store::ImapStore>(
    store: &S,
    principal: &lpe_mail_auth::AccountPrincipal,
    mailbox_name: &str,
    identifier: &str,
    grantee_account_id: uuid::Uuid,
    sender_right: SenderDelegationRight,
    should_exist: bool,
    exists: bool,
) -> Result<()> {
    let action_name = match sender_right {
        SenderDelegationRight::SendAs => "send-as",
        SenderDelegationRight::SendOnBehalf => "send-on-behalf",
    };
    if should_exist {
        store
            .upsert_sender_delegation_grant(
                SenderDelegationGrantInput {
                    owner_account_id: principal.account_id,
                    grantee_email: identifier.to_string(),
                    sender_right,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: format!("imap-setacl-{action_name}"),
                    subject: format!("set acl {} {}", mailbox_name, identifier),
                },
            )
            .await?;
    } else if exists {
        store
            .delete_sender_delegation_grant(
                principal.account_id,
                grantee_account_id,
                sender_right,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: format!("imap-deleteacl-{action_name}"),
                    subject: format!("set acl {} {}", mailbox_name, identifier),
                },
            )
            .await?;
    }
    Ok(())
}

use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, MailboxAccountAccess, SavedDraftMessage,
    SubmitMessageInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::{map_existing_recipients, map_recipients, select_from_addresses},
    drafts::{parse_draft_mutation, parse_email_copy},
    error::{method_error, set_error},
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, EmailCopyArguments, EmailGetArguments, EmailImportArguments,
        EmailQueryArguments, EmailQueryFilter, EmailQuerySort, EmailSetArguments,
        EmailSubmissionGetArguments, EmailSubmissionQueryArguments, EmailSubmissionQueryFilter,
        EmailSubmissionQuerySort, EmailSubmissionSetArguments, IdentityGetArguments,
        QueryChangesArguments, QuotaGetArguments, SearchSnippetGetArguments, ThreadGetArguments,
        ThreadQueryArguments,
    },
    state::{
        changes_response, changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state_reference, query_changes_response,
        query_changes_response_from_diff, query_diff_for_kind, query_position, state_cursor,
        validate_query_state_token, DurableObjectChange,
    },
    validation::validate_query_sort,
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT, SESSION_STATE,
};

mod import_validation;
mod imports;
mod values;

pub(crate) use values::*;

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_email_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(|value| parse_uuid(&value))
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let requested_position = arguments.position;
        let storage_position = if arguments.anchor.is_some()
            || requested_position.is_some_and(|position| position < 0)
        {
            0
        } else {
            requested_position.unwrap_or(0) as u64
        };
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, storage_position, limit)
            .await?;
        let full_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let position = query_position(
            &full_ids,
            requested_position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let ids = full_ids
            .iter()
            .skip(position)
            .take(limit as usize)
            .cloned()
            .collect::<Vec<_>>();
        let filter_state = arguments
            .filter
            .as_ref()
            .map(serialize_email_query_filter)
            .transpose()?;
        let sort_state = arguments
            .sort
            .as_ref()
            .map(|sort| serialize_email_query_sort(sort))
            .transpose()?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        let query_state = match self
            .store
            .save_jmap_query_state(
                account_id,
                "Email/query",
                filter_state.clone(),
                sort_state.clone(),
                cursor,
                &full_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                "Email/query",
                filter_state,
                sort_state,
                state_id,
                cursor,
            )?,
            None => crate::encode_query_state(
                account_id,
                "Email/query",
                filter_state,
                sort_state,
                full_ids,
            )?,
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": query.total,
        }))
    }

    pub(crate) async fn handle_email_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<EmailQueryFilter, EmailQuerySort> =
            serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(parse_uuid)
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let filter_state = arguments
            .filter
            .as_ref()
            .map(serialize_email_query_filter)
            .transpose()?;
        let sort_state = arguments
            .sort
            .as_ref()
            .map(|sort| serialize_email_query_sort(sort))
            .transpose()?;
        let previous = decode_query_state(&arguments.since_query_state)?;
        validate_query_state_token(
            account_id,
            "Email/query",
            filter_state.as_ref(),
            sort_state.as_ref(),
            &previous,
        )?;
        let mut previous_cursor = previous.cursor.unwrap_or(0);
        let previous_ids =
            if let Some(state_id) = previous.state_id.as_deref().map(parse_uuid).transpose()? {
                let stored = self
                    .store
                    .fetch_jmap_query_state(
                        account_id,
                        "Email/query",
                        state_id,
                        filter_state.clone(),
                        sort_state.clone(),
                    )
                    .await?
                    .ok_or_else(|| anyhow!("queryState is no longer available"))?;
                previous_cursor = stored.last_change_sequence;
                stored.snapshot_ids
            } else {
                previous.ids.clone()
            };
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, 0, MAX_QUERY_LIMIT)
            .await?;
        let current_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        let diff = query_diff_for_kind(
            "Email/query",
            &previous_ids,
            &current_ids,
            arguments.max_changes,
        );
        let next_cursor = if diff.has_more_changes {
            previous_cursor
        } else {
            cursor
        };
        let next_query_state = match self
            .store
            .save_jmap_query_state(
                account_id,
                "Email/query",
                filter_state.clone(),
                sort_state.clone(),
                next_cursor,
                &diff.query_state_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                "Email/query",
                filter_state.clone(),
                sort_state.clone(),
                state_id,
                next_cursor,
            )?,
            None => crate::encode_query_state(
                account_id,
                "Email/query",
                filter_state.clone(),
                sort_state.clone(),
                diff.query_state_ids.clone(),
            )?,
        };
        query_changes_response_from_diff(
            account_id,
            "Email/query",
            arguments.since_query_state,
            filter_state,
            sort_state,
            previous,
            next_query_state,
            query.total,
            diff,
        )
    }

    pub(crate) async fn handle_email_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let body_options = EmailBodyOptions::from_arguments(&arguments);
        let properties = email_properties(arguments.properties);

        let ids = match parse_uuid_list(arguments.ids)? {
            Some(ids) => ids,
            None => {
                self.store
                    .query_jmap_email_ids(account_id, None, None, 0, DEFAULT_GET_LIMIT)
                    .await?
                    .ids
            }
        };

        let emails = if account_access.is_owned && properties.contains("bcc") {
            self.store
                .fetch_jmap_emails_with_protected_bcc(account_id, &ids)
                .await?
        } else {
            self.store.fetch_jmap_emails(account_id, &ids).await?
        };
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.mail_object_state(&account_access, "Email").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": emails
                .iter()
                .map(|email| {
                    email_to_value(
                        email,
                        &properties,
                        &body_options,
                        account_access.is_owned
                            && email
                                .mailbox_states
                                .iter()
                                .any(|state| matches!(state.role.as_str(), "drafts" | "sent")),
                    )
                })
                .collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_email_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self
            .mail_object_state_entries(&account_access, "Email")
            .await?;
        let cursor = self.store.fetch_jmap_mail_change_cursor(account_id).await?;
        if let Some(after_cursor) = state_cursor(account_id, "Email", &arguments.since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_mail_object_changes(
                    account_id,
                    "Email",
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    "Email",
                    &arguments.since_state,
                    arguments.max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id.to_string(),
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            "Email",
            &arguments.since_state,
            arguments.max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn handle_email_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailCopyArguments = serde_json::from_value(arguments)?;
        let from_account_access = self
            .requested_account_access(account, Some(&arguments.from_account_id))
            .await?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let from_account_id = from_account_access.account_id;
        let account_id = account_access.account_id;
        if from_account_id != account_id {
            bail!("cross-account Email/copy is not supported");
        }
        let may_write = crate::mailboxes::mailbox_account_may_write(&account_access);

        let old_state = self.mail_object_state(&account_access, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        for (creation_id, value) in arguments.create {
            let copy_result = match crate::mailboxes::ensure_mailbox_write(may_write)
                .and_then(|_| parse_email_copy(value, created_ids))
            {
                Ok((email_id, mailbox_id)) => match self
                    .ensure_target_mailbox_accepts_message_write(
                        account_id,
                        mailbox_id,
                        &account_access,
                    )
                    .await
                {
                    Ok(()) => Ok((email_id, mailbox_id)),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            match copy_result {
                Ok((email_id, mailbox_id)) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-copy".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self
                        .store
                        .copy_jmap_email(account_id, email_id, mailbox_id, audit)
                        .await
                    {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": crate::blob_id_for_message(&email),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        let new_state = self.mail_object_state(&account_access, "Email").await?;
        Ok(json!({
            "fromAccountId": from_account_id.to_string(),
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    pub(crate) async fn handle_email_import(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailImportArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let may_write = crate::mailboxes::mailbox_account_may_write(&account_access);
        let old_state = self.mail_object_state(&account_access, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        for (creation_id, value) in arguments.emails {
            let import_result = match crate::mailboxes::ensure_mailbox_write(may_write) {
                Ok(()) => {
                    self.parse_email_import(account, &account_access, value, created_ids)
                        .await
                }
                Err(error) => Err(error),
            };
            match import_result {
                Ok(input) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-import".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self.store.import_jmap_email(input, audit).await {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": crate::blob_id_for_message(&email),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        let new_state = self.mail_object_state(&account_access, "Email").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    pub(crate) async fn handle_email_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let old_state = self.mail_object_state(&account_access, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                let create_result =
                    match crate::mailboxes::ensure_mailbox_draft_write(&account_access) {
                        Ok(()) => {
                            self.create_draft(account, &account_access, value, creation_id.as_str())
                                .await
                        }
                        Err(error) => Err(error),
                    };
                match create_result {
                    Ok(saved) => {
                        created_ids.insert(creation_id.clone(), saved.message_id.to_string());
                        created.insert(
                            creation_id,
                            json!({
                                "id": saved.message_id.to_string(),
                                "blobId": format!("draft:{}", saved.message_id),
                            }),
                        );
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                let update_result =
                    match crate::mailboxes::ensure_mailbox_draft_write(&account_access) {
                        Ok(()) => {
                            self.update_draft(account, &account_access, &id, value)
                                .await
                        }
                        Err(error) => Err(error),
                    };
                match update_result {
                    Ok(_) => {
                        updated.insert(id, Value::Object(Map::new()));
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match crate::mailboxes::ensure_mailbox_draft_write(&account_access)
                    .and_then(|_| parse_uuid(&id))
                {
                    Ok(message_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-draft-delete".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .delete_draft_message(account_id, message_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.mail_object_state(&account_access, "Email").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_email_submission_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSubmissionSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        if !account_access.is_owned && !account_access.may_write {
            bail!("write access is required to submit drafts from a delegated mailbox");
        }
        if !crate::mailboxes::mailbox_account_may_submit(&account_access) {
            bail!("sender delegation is required to submit from a delegated mailbox");
        }
        let old_state = self.email_submission_object_state(account_id).await?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match crate::parse_submission_email_id(&value, created_ids)? {
                    Some(email_id) => {
                        let message_id = parse_uuid(&email_id)?;
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-submit".to_string(),
                            subject: email_id.clone(),
                        };
                        match self
                            .store
                            .submit_draft_message(
                                account_id,
                                message_id,
                                account.account_id,
                                "jmap",
                                audit,
                            )
                            .await
                        {
                            Ok(result) => {
                                created_ids.insert(
                                    creation_id.clone(),
                                    result.outbound_queue_id.to_string(),
                                );
                                created.insert(
                                    creation_id,
                                    json!({
                                        "id": result.outbound_queue_id.to_string(),
                                        "emailId": result.message_id.to_string(),
                                        "threadId": result.thread_id.to_string(),
                                        "undoStatus": "final",
                                    }),
                                );
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    None => {
                        not_created.insert(
                            creation_id,
                            method_error("invalidArguments", "emailId is required"),
                        );
                    }
                }
            }
        }

        let new_state = self.email_submission_object_state(account_id).await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    pub(crate) async fn handle_email_submission_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailSubmissionGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let ids = parse_uuid_list(arguments.ids)?;
        let properties = email_submission_properties(arguments.properties);
        let submissions = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            if ids.as_ref().is_some_and(Vec::is_empty) {
                Vec::new()
            } else {
                self.store
                    .fetch_jmap_email_submissions(account_id, ids.as_deref().unwrap_or(&[]))
                    .await?
            }
        } else {
            Vec::new()
        };
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !submissions.iter().any(|submission| submission.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.email_submission_object_state(account_id).await?
        } else {
            crate::state::encode_state(account_id, "EmailSubmission", Vec::new())?
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": submissions
                .iter()
                .map(|submission| email_submission_to_value(submission, &properties))
                .collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_email_submission_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailSubmissionQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_email_submission_query(arguments.filter.as_ref(), arguments.sort.as_deref())?;
        let mut submissions = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.store
                .fetch_jmap_email_submissions(account_id, &[])
                .await?
        } else {
            Vec::new()
        };
        apply_email_submission_query(
            &mut submissions,
            arguments.filter.as_ref(),
            arguments.sort.as_deref(),
        );

        let current_ids = submissions
            .iter()
            .map(|submission| submission.id.to_string())
            .collect::<Vec<_>>();
        let position = query_position(
            &current_ids,
            arguments.position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = current_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "EmailSubmission/query",
                arguments.filter.map(serde_json::to_value).transpose()?,
                serialize_email_submission_query_sort(arguments.sort.as_deref())?,
                current_ids,
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": submissions.len(),
        }))
    }

    pub(crate) async fn handle_email_submission_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<EmailSubmissionQueryFilter, EmailSubmissionQuerySort> =
            serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_email_submission_query(arguments.filter.as_ref(), arguments.sort.as_deref())?;
        let mut submissions = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.store
                .fetch_jmap_email_submissions(account_id, &[])
                .await?
        } else {
            Vec::new()
        };
        apply_email_submission_query(
            &mut submissions,
            arguments.filter.as_ref(),
            arguments.sort.as_deref(),
        );
        let current_ids = submissions
            .iter()
            .map(|submission| submission.id.to_string())
            .collect::<Vec<_>>();

        query_changes_response(
            account_id,
            "EmailSubmission/query",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            serialize_email_submission_query_sort(arguments.sort.as_deref())?,
            current_ids,
            submissions.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_email_submission_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.email_submission_object_state_entries(account_id)
                .await?
        } else {
            Vec::new()
        };
        self.object_changes_response(
            account_id,
            "EmailSubmission",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
        .await
    }

    pub(crate) async fn handle_identity_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: IdentityGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = identity_properties(arguments.properties);
        let identities = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.store
                .fetch_sender_identities(account.account_id, account_id)
                .await?
        } else {
            Vec::new()
        };
        let requested_ids = arguments.ids;
        let list = identities
            .iter()
            .filter(|identity| {
                requested_ids
                    .as_ref()
                    .map(|ids| ids.contains(&identity.id))
                    .unwrap_or(true)
            })
            .map(|identity| identity_to_value(identity, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !identities.iter().any(|identity| identity.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.identity_object_state(account.account_id, account_id)
                .await?
        } else {
            crate::state::encode_state(account_id, "Identity", Vec::new())?
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_identity_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = if crate::mailboxes::mailbox_account_may_submit(&account_access) {
            self.identity_object_state_entries(account.account_id, account_id)
                .await?
        } else {
            Vec::new()
        };
        changes_response(
            account_id,
            "Identity",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
    }

    pub(crate) async fn handle_thread_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(parse_uuid)
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let requested_position = arguments.position;
        let storage_position = if arguments.anchor.is_some()
            || requested_position.is_some_and(|position| position < 0)
        {
            0
        } else {
            requested_position.unwrap_or(0) as u64
        };
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_thread_ids(account_id, mailbox_id, search_text, storage_position, limit)
            .await?;
        let full_ids = self
            .resolve_full_thread_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let position = query_position(
            &full_ids,
            requested_position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let ids = full_ids
            .iter()
            .skip(position)
            .take(limit as usize)
            .cloned()
            .collect::<Vec<_>>();
        let query_state = crate::encode_query_state(
            account_id,
            "Thread/query",
            arguments
                .filter
                .as_ref()
                .map(serialize_email_query_filter)
                .transpose()?,
            arguments
                .sort
                .as_ref()
                .map(|sort| serialize_email_query_sort(sort))
                .transpose()?,
            full_ids,
        )?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": query.total,
        }))
    }

    pub(crate) async fn handle_thread_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<EmailQueryFilter, EmailQuerySort> =
            serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(parse_uuid)
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let query = self
            .store
            .query_jmap_thread_ids(account_id, mailbox_id, search_text, 0, MAX_QUERY_LIMIT)
            .await?;
        let current_ids = self
            .resolve_full_thread_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        query_changes_response(
            account_id,
            "Thread/query",
            arguments.since_query_state,
            arguments
                .filter
                .as_ref()
                .map(serialize_email_query_filter)
                .transpose()?,
            arguments
                .sort
                .as_ref()
                .map(|sort| serialize_email_query_sort(sort))
                .transpose()?,
            current_ids,
            query.total,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_thread_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = thread_properties(arguments.properties);
        let all_email_ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self
            .store
            .fetch_jmap_emails(account_id, &all_email_ids)
            .await?;
        let ids = arguments.ids.unwrap_or_else(|| {
            emails
                .iter()
                .map(|email| email.thread_id.to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        });

        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            let thread_id = parse_uuid(&id)?;
            let thread_emails = emails
                .iter()
                .filter(|email| email.thread_id == thread_id)
                .map(|email| email.id.to_string())
                .collect::<Vec<_>>();
            if thread_emails.is_empty() {
                not_found.push(Value::String(id));
            } else {
                list.push(thread_to_value(thread_id, thread_emails, &properties));
            }
        }
        let state = self.mail_object_state(&account_access, "Thread").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_thread_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self
            .mail_object_state_entries(&account_access, "Thread")
            .await?;
        let cursor = self.store.fetch_jmap_mail_change_cursor(account_id).await?;
        if let Some(after_cursor) = state_cursor(account_id, "Thread", &arguments.since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_mail_object_changes(
                    account_id,
                    "Thread",
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    "Thread",
                    &arguments.since_state,
                    arguments.max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id.to_string(),
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            "Thread",
            &arguments.since_state,
            arguments.max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn handle_quota_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QuotaGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let quota = self.store.fetch_jmap_quota(account_id).await?;
        let ids = arguments.ids.unwrap_or_else(|| vec![quota.id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == quota.id {
                list.push(quota_to_value(&quota));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_search_snippet_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: SearchSnippetGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let ids = parse_uuid_list(Some(arguments.email_ids))?.unwrap_or_default();
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "list": emails.iter().map(search_snippet_to_value).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    pub(crate) async fn resolve_full_email_query_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        query: &lpe_storage::JmapEmailQuery,
    ) -> Result<Vec<String>> {
        let ids = if query.total > query.ids.len() as u64 {
            self.store
                .query_jmap_email_ids(
                    account_id,
                    mailbox_id,
                    search_text,
                    0,
                    full_query_limit(query.total),
                )
                .await?
                .ids
        } else {
            query.ids.clone()
        };

        Ok(ids.into_iter().map(|id| id.to_string()).collect())
    }

    pub(crate) async fn resolve_full_thread_query_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        query: &lpe_storage::JmapThreadQuery,
    ) -> Result<Vec<String>> {
        let ids = if query.total > query.ids.len() as u64 {
            self.store
                .query_jmap_thread_ids(
                    account_id,
                    mailbox_id,
                    search_text,
                    0,
                    full_query_limit(query.total),
                )
                .await?
                .ids
        } else {
            query.ids.clone()
        };

        Ok(ids.into_iter().map(|id| id.to_string()).collect())
    }

    pub(crate) async fn create_draft(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        value: Value,
        creation_id: &str,
    ) -> Result<SavedDraftMessage> {
        let mutation = parse_draft_mutation(value)?;
        let (from, sender) =
            select_from_addresses(mutation.from, mutation.sender, account, account_access)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-create".to_string(),
            subject: creation_id.to_string(),
        };
        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: account_access.account_id,
                    submitted_by_account_id: account.account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    sender_display: sender.as_ref().and_then(|value| value.name.clone()),
                    sender_address: sender.map(|value| value.email),
                    to: map_recipients(mutation.to.unwrap_or_default())?,
                    cc: map_recipients(mutation.cc.unwrap_or_default())?,
                    bcc: map_recipients(mutation.bcc.unwrap_or_default())?,
                    subject: mutation.subject.unwrap_or_default(),
                    body_text: mutation.text_body.unwrap_or_default(),
                    body_html_sanitized: mutation.html_body.unwrap_or(None),
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: 0,
                    unread: Some(mutation.unread.unwrap_or(false)),
                    flagged: Some(mutation.flagged.unwrap_or(false)),
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    pub(crate) async fn update_draft(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        id: &str,
        value: Value,
    ) -> Result<SavedDraftMessage> {
        let message_id = parse_uuid(id)?;
        let existing = self
            .store
            .fetch_jmap_emails_with_protected_bcc(account_access.account_id, &[message_id])
            .await?
            .into_iter()
            .find(|email| {
                email
                    .mailbox_states
                    .iter()
                    .any(|state| state.role == "drafts")
            })
            .ok_or_else(|| anyhow!("draft not found"))?;
        let mutation = parse_draft_mutation(value)?;
        let (from, sender) =
            select_from_addresses(mutation.from, mutation.sender, account, account_access)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-update".to_string(),
            subject: id.to_string(),
        };

        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: Some(message_id),
                    account_id: account_access.account_id,
                    submitted_by_account_id: account.account_id,
                    source: "jmap".to_string(),
                    from_display: from.name.or(existing.from_display.clone()),
                    from_address: if from.email.trim().is_empty() {
                        existing.from_address
                    } else {
                        from.email
                    },
                    sender_display: sender
                        .as_ref()
                        .and_then(|value| value.name.clone())
                        .or(existing.sender_display),
                    sender_address: sender.map(|value| value.email).or(existing.sender_address),
                    to: mutation
                        .to
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.to)),
                    cc: mutation
                        .cc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.cc)),
                    bcc: mutation
                        .bcc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.bcc)),
                    subject: mutation.subject.unwrap_or(existing.subject),
                    body_text: mutation.text_body.unwrap_or(existing.body_text),
                    body_html_sanitized: mutation.html_body.unwrap_or(existing.body_html_sanitized),
                    internet_message_id: existing.internet_message_id,
                    mime_blob_ref: None,
                    size_octets: existing.size_octets,
                    unread: Some(mutation.unread.unwrap_or(existing.unread)),
                    flagged: Some(mutation.flagged.unwrap_or(existing.flagged)),
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }
}

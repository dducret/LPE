use anyhow::{anyhow, bail, Result};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest};
use lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, AuthenticatedAccount, JmapEmail,
    JmapEmailSubmission, JmapImportedEmailInput, JmapQuota, MailboxAccountAccess,
    SavedDraftMessage, SenderIdentity, SubmitMessageInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::{
        address_value, insert_if, map_existing_recipients, map_parsed_recipients, map_recipients,
        select_from_addresses,
    },
    drafts::{parse_draft_mutation, parse_email_copy},
    error::{method_error, set_error},
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, EmailCopyArguments, EmailGetArguments, EmailImportArguments,
        EmailQueryArguments, EmailQueryFilter, EmailQuerySort, EmailSetArguments,
        EmailSubmissionGetArguments, EmailSubmissionSetArguments, IdentityGetArguments,
        QueryChangesArguments, QuotaGetArguments, SearchSnippetGetArguments, ThreadGetArguments,
        ThreadQueryArguments,
    },
    state::{changes_response, query_changes_response},
    upload::{expected_attachment_kind, parse_upload_blob_id},
    validation::validate_query_sort,
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT, SESSION_STATE,
};

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
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;
        let full_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let ids = query
            .ids
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        let query_state = crate::encode_query_state(
            account_id,
            "Email/query",
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
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, 0, MAX_QUERY_LIMIT)
            .await?;
        let current_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        query_changes_response(
            account_id,
            "Email/query",
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

        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Email").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": emails
                .iter()
                .map(|email| {
                    email_to_value(
                        email,
                        &properties,
                        account_access.is_owned
                            && matches!(email.mailbox_role.as_str(), "drafts" | "sent"),
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
        let entries = self.object_state_entries(account_id, "Email").await?;
        changes_response(
            account_id,
            "Email",
            &arguments.since_state,
            arguments.max_changes,
            entries,
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

        let old_state = self.object_state(account_id, "Email").await?;
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

        let new_state = self.object_state(account_id, "Email").await?;
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
        let old_state = self.object_state(account_id, "Email").await?;
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

        let new_state = self.object_state(account_id, "Email").await?;
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
        let old_state = self.object_state(account_id, "Email").await?;
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

        let new_state = self.object_state(account_id, "Email").await?;
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
        let old_state = self.object_state(account_id, "Email").await?;
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

        let new_state = self.object_state(account_id, "Email").await?;
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
        let ids_ref = ids.as_deref().unwrap_or(&[]);
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, ids_ref)
            .await?;
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !submissions.iter().any(|submission| submission.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Email").await?;

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
        let requested_ids = arguments.ids.unwrap_or_default();
        let list = identities
            .iter()
            .filter(|identity| requested_ids.is_empty() || requested_ids.contains(&identity.id))
            .map(|identity| identity_to_value(identity, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .into_iter()
            .filter(|id| !identities.iter().any(|identity| identity.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Identity").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
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
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_thread_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;
        let full_ids = self
            .resolve_full_thread_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let ids = query
            .ids
            .into_iter()
            .map(|id| id.to_string())
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
            "canCalculateChanges": false,
            "position": position,
            "ids": ids,
            "total": query.total,
        }))
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
        let state = self.object_state(account_id, "Thread").await?;

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
        let entries = self.object_state_entries(account_id, "Thread").await?;
        changes_response(
            account_id,
            "Thread",
            &arguments.since_state,
            arguments.max_changes,
            entries,
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
            .fetch_jmap_draft(account_access.account_id, message_id)
            .await?
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

    pub(crate) async fn parse_email_import(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        value: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<JmapImportedEmailInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("import arguments must be an object"))?;
        let blob_id = object
            .get("blobId")
            .and_then(Value::as_str)
            .map(|value| crate::resolve_creation_reference(value, created_ids))
            .ok_or_else(|| anyhow!("blobId is required"))?;
        let blob_id = parse_upload_blob_id(&blob_id)?;
        let mailbox_ids = object
            .get("mailboxIds")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mailboxIds is required"))?;
        let target_mailbox_id = mailbox_ids
            .iter()
            .find(|(_, included)| included.as_bool().unwrap_or(false))
            .map(|(mailbox_id, _)| parse_uuid(mailbox_id))
            .transpose()?
            .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
        self.ensure_target_mailbox_accepts_message_write(
            account_access.account_id,
            target_mailbox_id,
            account_access,
        )
        .await?;
        let blob = self
            .store
            .fetch_jmap_upload_blob(account_access.account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("uploaded blob not found"))?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapEmailImport,
                declared_mime: Some(blob.media_type.clone()),
                filename: None,
                expected_kind: ExpectedKind::Rfc822Message,
            },
            &blob.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP email import blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let parsed = parse_rfc822_message(&blob.blob_bytes)?;
        self.validate_imported_attachments(&parsed.attachments)?;

        Ok(JmapImportedEmailInput {
            account_id: account_access.account_id,
            submitted_by_account_id: account.account_id,
            mailbox_id: target_mailbox_id,
            source: "jmap-import".to_string(),
            from_display: parsed
                .from
                .as_ref()
                .and_then(|from| from.display_name.clone())
                .or(Some(account_access.display_name.clone())),
            from_address: parsed
                .from
                .map(|from| from.email)
                .unwrap_or_else(|| account_access.email.clone()),
            sender_display: None,
            sender_address: None,
            to: map_parsed_recipients(parsed.to),
            cc: map_parsed_recipients(parsed.cc),
            bcc: Vec::new(),
            subject: parsed.subject,
            body_text: parsed.body_text,
            body_html_sanitized: parsed.body_html_sanitized,
            internet_message_id: parsed.message_id,
            mime_blob_ref: format!("upload:{}", blob.id),
            size_octets: blob.octet_size as i64,
            received_at: None,
            attachments: parsed.attachments,
        })
    }

    pub(crate) fn validate_imported_attachments(
        &self,
        attachments: &[lpe_storage::AttachmentUploadInput],
    ) -> Result<()> {
        for attachment in attachments {
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::AttachmentParsing,
                    declared_mime: Some(attachment.media_type.clone()),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        attachment.media_type.as_str(),
                        attachment.file_name.as_str(),
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                bail!(
                    "JMAP email import attachment '{}' blocked by Magika validation: {}",
                    attachment.file_name,
                    outcome.reason
                );
            }
        }

        Ok(())
    }

    async fn ensure_target_mailbox_accepts_message_write(
        &self,
        account_id: Uuid,
        target_mailbox_id: Uuid,
        account_access: &MailboxAccountAccess,
    ) -> Result<()> {
        crate::mailboxes::ensure_mailbox_write(crate::mailboxes::mailbox_account_may_write(
            account_access,
        ))?;
        if let Some(target_mailbox) = self
            .store
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
        {
            if target_mailbox.role == "drafts" {
                crate::mailboxes::ensure_mailbox_draft_write(account_access)?;
            }
        }

        Ok(())
    }
}

pub(crate) fn full_query_limit(total: u64) -> u64 {
    total.max(1).min(i64::MAX as u64)
}

pub(crate) fn serialize_email_query_filter(filter: &EmailQueryFilter) -> Result<Value> {
    Ok(serde_json::to_value(filter)?)
}

pub(crate) fn serialize_email_query_sort(sort: &[EmailQuerySort]) -> Result<Vec<Value>> {
    sort.iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn email_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "blobId".to_string(),
                "threadId".to_string(),
                "mailboxIds".to_string(),
                "keywords".to_string(),
                "size".to_string(),
                "receivedAt".to_string(),
                "sentAt".to_string(),
                "messageId".to_string(),
                "subject".to_string(),
                "from".to_string(),
                "sender".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "preview".to_string(),
                "hasAttachment".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
                "bodyValues".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn email_submission_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "emailId".to_string(),
                "threadId".to_string(),
                "identityId".to_string(),
                "envelope".to_string(),
                "sendAt".to_string(),
                "undoStatus".to_string(),
                "deliveryStatus".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn identity_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "replyTo".to_string(),
                "bcc".to_string(),
                "textSignature".to_string(),
                "htmlSignature".to_string(),
                "mayDelete".to_string(),
                "xLpeOwnerAccountId".to_string(),
                "xLpeAuthorizationKind".to_string(),
                "xLpeSender".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

pub(crate) fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| vec!["id".to_string(), "emailIds".to_string()])
        .into_iter()
        .collect()
}

pub(crate) fn email_to_value(
    email: &JmapEmail,
    properties: &HashSet<String>,
    include_owner_bcc: bool,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", email.id.to_string());
    insert_if(
        properties,
        &mut object,
        "blobId",
        crate::blob_id_for_message(email),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        email.thread_id.to_string(),
    );
    if properties.contains("mailboxIds") {
        let mut mailbox_ids = Map::new();
        mailbox_ids.insert(email.mailbox_id.to_string(), Value::Bool(true));
        object.insert("mailboxIds".to_string(), Value::Object(mailbox_ids));
    }
    if properties.contains("keywords") {
        object.insert("keywords".to_string(), email_keywords(email));
    }
    insert_if(properties, &mut object, "size", email.size_octets);
    insert_if(
        properties,
        &mut object,
        "receivedAt",
        email.received_at.clone(),
    );
    if let Some(sent_at) = &email.sent_at {
        insert_if(properties, &mut object, "sentAt", sent_at.clone());
    }
    if properties.contains("messageId") {
        object.insert(
            "messageId".to_string(),
            Value::Array(
                email
                    .internet_message_id
                    .as_ref()
                    .map(|message_id| vec![Value::String(message_id.clone())])
                    .unwrap_or_default(),
            ),
        );
    }
    insert_if(properties, &mut object, "subject", email.subject.clone());
    if properties.contains("from") {
        object.insert(
            "from".to_string(),
            Value::Array(vec![address_value(
                &email.from_address,
                email.from_display.as_deref(),
            )]),
        );
    }
    if properties.contains("sender") && email.sender_address.is_some() {
        object.insert(
            "sender".to_string(),
            Value::Array(vec![address_value(
                email.sender_address.as_deref().unwrap_or_default(),
                email.sender_display.as_deref(),
            )]),
        );
    }
    if properties.contains("to") {
        object.insert(
            "to".to_string(),
            Value::Array(
                email
                    .to
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("cc") {
        object.insert(
            "cc".to_string(),
            Value::Array(
                email
                    .cc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if include_owner_bcc && properties.contains("bcc") && !email.bcc.is_empty() {
        object.insert(
            "bcc".to_string(),
            Value::Array(
                email
                    .bcc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    insert_if(properties, &mut object, "preview", email.preview.clone());
    insert_if(
        properties,
        &mut object,
        "hasAttachment",
        email.has_attachments,
    );

    let mut body_values = Map::new();
    if !email.body_text.is_empty() {
        body_values.insert(
            "textBody".to_string(),
            json!({
                "value": email.body_text.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("textBody") {
            object.insert(
                "textBody".to_string(),
                json!([{ "partId": "textBody", "type": "text/plain" }]),
            );
        }
    }
    if let Some(html) = &email.body_html_sanitized {
        body_values.insert(
            "htmlBody".to_string(),
            json!({
                "value": html.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("htmlBody") {
            object.insert(
                "htmlBody".to_string(),
                json!([{ "partId": "htmlBody", "type": "text/html" }]),
            );
        }
    }
    if properties.contains("bodyValues") {
        object.insert("bodyValues".to_string(), Value::Object(body_values));
    }

    Value::Object(object)
}

pub(crate) fn email_submission_to_value(
    submission: &JmapEmailSubmission,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", submission.id.to_string());
    insert_if(
        properties,
        &mut object,
        "emailId",
        submission.email_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        submission.thread_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "identityId",
        submission.identity_id.clone(),
    );
    if properties.contains("envelope") {
        object.insert(
            "envelope".to_string(),
            json!({
                "mailFrom": {"email": submission.envelope_mail_from},
                "rcptTo": submission.envelope_rcpt_to.iter().map(|address| json!({"email": address})).collect::<Vec<_>>(),
            }),
        );
    }
    insert_if(
        properties,
        &mut object,
        "sendAt",
        submission.send_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "undoStatus",
        submission.undo_status.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "deliveryStatus",
        submission.delivery_status.clone(),
    );
    Value::Object(object)
}

pub(crate) fn identity_to_value(identity: &SenderIdentity, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", identity.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        identity.display_name.clone(),
    );
    insert_if(properties, &mut object, "email", identity.email.clone());
    if properties.contains("replyTo") {
        object.insert("replyTo".to_string(), Value::Null);
    }
    if properties.contains("bcc") {
        object.insert("bcc".to_string(), Value::Null);
    }
    insert_if(properties, &mut object, "textSignature", "");
    insert_if(properties, &mut object, "htmlSignature", "");
    insert_if(properties, &mut object, "mayDelete", false);
    insert_if(
        properties,
        &mut object,
        "xLpeOwnerAccountId",
        identity.owner_account_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "xLpeAuthorizationKind",
        identity.authorization_kind.clone(),
    );
    if properties.contains("xLpeSender") {
        let sender = identity.sender_address.as_ref().map(|address| {
            json!({
                "email": address,
                "name": identity.sender_display.clone(),
            })
        });
        object.insert("xLpeSender".to_string(), sender.unwrap_or(Value::Null));
    }
    Value::Object(object)
}

pub(crate) fn thread_to_value(
    thread_id: Uuid,
    email_ids: Vec<String>,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", thread_id.to_string());
    if properties.contains("emailIds") {
        object.insert(
            "emailIds".to_string(),
            Value::Array(email_ids.into_iter().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

pub(crate) fn search_snippet_to_value(email: &JmapEmail) -> Value {
    let subject = if email.subject.is_empty() {
        email.preview.clone()
    } else {
        email.subject.clone()
    };
    let preview = if email.preview.is_empty() {
        crate::trim_snippet(&email.body_text, 120)
    } else {
        crate::trim_snippet(&email.preview, 120)
    };
    json!({
        "emailId": email.id.to_string(),
        "subject": subject,
        "preview": preview,
    })
}

pub(crate) fn quota_to_value(quota: &JmapQuota) -> Value {
    json!({
        "id": quota.id,
        "name": quota.name,
        "used": quota.used,
        "hardLimit": quota.hard_limit,
        "scope": "account",
    })
}

pub(crate) fn email_keywords(email: &JmapEmail) -> Value {
    let mut keywords = Map::new();
    if email.mailbox_role == "drafts" {
        keywords.insert("$draft".to_string(), Value::Bool(true));
    }
    if !email.unread {
        keywords.insert("$seen".to_string(), Value::Bool(true));
    }
    if email.flagged {
        keywords.insert("$flagged".to_string(), Value::Bool(true));
    }
    Value::Object(keywords)
}

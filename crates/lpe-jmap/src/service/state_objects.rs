use super::*;

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn object_state(&self, account_id: Uuid, data_type: &str) -> Result<String> {
        let entries = self.object_state_entries(account_id, data_type).await?;
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        encode_state_with_cursor(account_id, data_type, entries, cursor)
    }

    pub(crate) async fn object_changes_response(
        &self,
        account_id: Uuid,
        data_type: &str,
        since_state: &str,
        max_changes: Option<u64>,
        entries: Vec<StateEntry>,
    ) -> Result<Value> {
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        if let Some(after_cursor) = state_cursor(account_id, data_type, since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_object_changes(
                    account_id,
                    data_type,
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
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
            data_type,
            since_state,
            max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn mailbox_object_state(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<String> {
        let entries = self.mailbox_object_state_entries(access).await?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(access.account_id)
            .await?;
        encode_state_with_cursor(access.account_id, "Mailbox", entries, cursor)
    }

    pub(crate) async fn mailbox_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<Vec<StateEntry>> {
        let mailboxes = self.store.fetch_jmap_mailboxes(access.account_id).await?;
        Ok(mailboxes
            .into_iter()
            .map(|mailbox| StateEntry {
                id: mailbox.id.to_string(),
                fingerprint: mailbox_state_fingerprint(&mailbox, Some(access)),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<String> {
        let entries = self.mail_object_state_entries(access, data_type).await?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(access.account_id)
            .await?;
        encode_state_with_cursor(access.account_id, data_type, entries, cursor)
    }

    pub(crate) async fn email_delivery_object_state(&self, account_id: Uuid) -> Result<String> {
        let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let entries = emails
            .into_iter()
            .map(|email| StateEntry {
                id: email.id.to_string(),
                fingerprint: opaque_state_fingerprint(&email.received_at),
            })
            .collect();
        encode_state(account_id, "EmailDelivery", entries)
    }

    pub(crate) async fn email_submission_object_state(&self, account_id: Uuid) -> Result<String> {
        let entries = self
            .email_submission_object_state_entries(account_id)
            .await?;
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, "EmailSubmission")
            .await?;
        encode_state_with_cursor(account_id, "EmailSubmission", entries, cursor)
    }

    pub(crate) async fn email_submission_object_state_entries(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, &[])
            .await?;
        Ok(submissions
            .into_iter()
            .map(|submission| StateEntry {
                id: submission.id.to_string(),
                fingerprint: email_submission_state_fingerprint(&submission),
            })
            .collect())
    }

    pub(crate) async fn identity_object_state(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<String> {
        let entries = self
            .identity_object_state_entries(principal_account_id, target_account_id)
            .await?;
        encode_state(target_account_id, "Identity", entries)
    }

    pub(crate) async fn identity_object_state_entries(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let identities = self
            .store
            .fetch_sender_identities(principal_account_id, target_account_id)
            .await?;
        Ok(identities
            .into_iter()
            .map(|identity| StateEntry {
                id: identity.id.clone(),
                fingerprint: identity_state_fingerprint(&identity),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        self.mail_object_state_entries_with_bcc(access.account_id, data_type, access.is_owned)
            .await
    }

    async fn mail_object_state_entries_with_bcc(
        &self,
        account_id: Uuid,
        data_type: &str,
        include_bcc: bool,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Email" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(account_id, &ids)
                        .await?
                } else {
                    self.store.fetch_jmap_emails(account_id, &ids).await?
                };
                Ok(emails
                    .into_iter()
                    .map(|email| StateEntry {
                        id: email.id.to_string(),
                        fingerprint: email_state_fingerprint(&email, include_bcc),
                    })
                    .collect())
            }
            "Thread" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(account_id, &ids)
                        .await?
                } else {
                    self.store.fetch_jmap_emails(account_id, &ids).await?
                };
                let mut threads: HashMap<Uuid, Vec<String>> = HashMap::new();
                for email in emails {
                    threads.entry(email.thread_id).or_default().push(format!(
                        "{}:{}",
                        email.id,
                        email_state_fingerprint(&email, include_bcc)
                    ));
                }
                let mut entries = threads
                    .into_iter()
                    .map(|(thread_id, mut fingerprints)| {
                        fingerprints.sort();
                        StateEntry {
                            id: thread_id.to_string(),
                            fingerprint: opaque_state_fingerprint(&fingerprints.join("|")),
                        }
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|left, right| left.id.cmp(&right.id));
                Ok(entries)
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn object_state_entries(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Mailbox" => {
                let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
                Ok(mailboxes
                    .into_iter()
                    .map(|mailbox| StateEntry {
                        id: mailbox.id.to_string(),
                        fingerprint: mailbox_state_fingerprint(&mailbox, None),
                    })
                    .collect())
            }
            "Email" | "Thread" => {
                self.mail_object_state_entries_with_bcc(account_id, data_type, true)
                    .await
            }
            "AddressBook" => {
                let collections = self
                    .store
                    .fetch_accessible_contact_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "ContactCard" => {
                let contacts = self.store.fetch_accessible_contacts(account_id).await?;
                Ok(contacts
                    .into_iter()
                    .map(|contact| StateEntry {
                        id: contact.id.to_string(),
                        fingerprint: contact_state_fingerprint(&contact),
                    })
                    .collect())
            }
            "Calendar" => {
                let collections = self
                    .store
                    .fetch_accessible_calendar_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "CalendarEvent" => {
                let events = self.store.fetch_accessible_events(account_id).await?;
                Ok(events
                    .into_iter()
                    .map(|event| StateEntry {
                        id: event.id.to_string(),
                        fingerprint: event_state_fingerprint(&event),
                    })
                    .collect())
            }
            "TaskList" => {
                let task_lists = self.store.fetch_jmap_task_lists(account_id).await?;
                Ok(task_lists
                    .into_iter()
                    .map(|task_list| StateEntry {
                        id: task_list.id.to_string(),
                        fingerprint: task_list_state_fingerprint(&task_list),
                    })
                    .collect())
            }
            "Task" => {
                let tasks = self.store.fetch_jmap_tasks(account_id).await?;
                Ok(tasks
                    .into_iter()
                    .map(|task| StateEntry {
                        id: task.id.to_string(),
                        fingerprint: task_state_fingerprint(&task),
                    })
                    .collect())
            }
            "Note" => {
                let notes = self.store.fetch_jmap_notes(account_id).await?;
                Ok(notes
                    .into_iter()
                    .map(|note| StateEntry {
                        id: note.id.to_string(),
                        fingerprint: crate::notes_journal::note_state_fingerprint(&note),
                    })
                    .collect())
            }
            "JournalEntry" => {
                let entries = self.store.fetch_jmap_journal_entries(account_id).await?;
                Ok(entries
                    .into_iter()
                    .map(|entry| StateEntry {
                        id: entry.id.to_string(),
                        fingerprint: crate::notes_journal::journal_entry_state_fingerprint(&entry),
                    })
                    .collect())
            }
            "Reminder" => {
                let reminders = self
                    .store
                    .query_jmap_reminders(
                        account_id,
                        lpe_storage::ReminderQuery {
                            include_inactive: true,
                        },
                    )
                    .await?;
                Ok(reminders
                    .into_iter()
                    .map(|reminder| StateEntry {
                        id: format!("{}:{}", reminder.source_type, reminder.source_id),
                        fingerprint: crate::notes_journal::reminder_state_fingerprint(&reminder),
                    })
                    .collect())
            }
            "Rule" => {
                let rules = self.store.list_mailbox_rules(account_id).await?;
                Ok(rules
                    .into_iter()
                    .map(|rule| StateEntry {
                        id: rule.id.to_string(),
                        fingerprint: opaque_state_fingerprint(&rule_to_value(rule).to_string()),
                    })
                    .collect())
            }
            "OutlookProfile" => {
                let profile = self.store.fetch_outlook_profile_state(account_id).await?;
                Ok(vec![StateEntry {
                    id: profile.id.clone(),
                    fingerprint: opaque_state_fingerprint(
                        &outlook_profile_state_to_value(profile).to_string(),
                    ),
                }])
            }
            "SearchFolder" => {
                let folders = self.store.fetch_search_folders(account_id).await?;
                Ok(folders
                    .into_iter()
                    .map(|folder| StateEntry {
                        id: folder.id.to_string(),
                        fingerprint: opaque_state_fingerprint(
                            &search_folder_to_value(folder).to_string(),
                        ),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }
}

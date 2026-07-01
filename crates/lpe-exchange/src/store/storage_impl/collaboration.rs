macro_rules! store_impl_collaboration {
    () => {
    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_contact_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_calendar_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_task_collections(principal_account_id)
                .await
        })
    }

    fn fetch_delegate_freebusy_messages<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<DelegateFreeBusyMessageObject>> {
        Box::pin(async move {
            self.fetch_delegate_freebusy_messages(principal_account_id, None)
                .await
        })
    }

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move {
            self.fetch_accessible_contacts_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_contact_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let contacts = self
                .fetch_accessible_contacts_in_collection(principal_account_id, collection_id)
                .await?;
            let ids = contacts
                .iter()
                .map(|contact| contact.id)
                .collect::<Vec<_>>();
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let rows = sqlx::query(
                r#"
                SELECT
                    id,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                FROM contacts
                WHERE id = ANY($1)
                "#,
            )
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| (row.get("id"), row.get("updated_at")))
                .collect())
        })
    }

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move {
            self.fetch_accessible_events_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_event_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let events = self
                .fetch_accessible_events_in_collection(principal_account_id, collection_id)
                .await?;
            let ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let rows = sqlx::query(
                r#"
                SELECT
                    id,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                FROM calendar_events
                WHERE id = ANY($1)
                "#,
            )
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| (row.get("id"), row.get("updated_at")))
                .collect())
        })
    }

    fn fetch_accessible_tasks_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        Box::pin(async move {
            let tasks = self.fetch_client_tasks(principal_account_id).await?;
            Ok(tasks
                .into_iter()
                .filter(|task| task_matches_collection(task, collection_id))
                .collect())
        })
    }

    fn fetch_task_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let tasks = self.fetch_client_tasks(principal_account_id).await?;
            Ok(tasks
                .into_iter()
                .filter(|task| task_matches_collection(task, collection_id))
                .map(|task| (task.id, task.updated_at))
                .collect())
        })
    }

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move {
            self.fetch_accessible_contacts_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.create_accessible_contact(principal_account_id, collection_id, input)
                .await
        })
    }

    fn update_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.update_accessible_contact(principal_account_id, contact_id, input)
                .await
        })
    }

    fn delete_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_contact(principal_account_id, contact_id)
                .await
        })
    }

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move {
            self.fetch_accessible_events_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.create_accessible_event(principal_account_id, collection_id, input)
                .await
        })
    }

    fn update_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.update_accessible_event(principal_account_id, event_id, input)
                .await
        })
    }

    fn update_accessible_event_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.update_accessible_event_reminder(
                principal_account_id,
                event_id,
                reminder_set,
                reminder_at,
                reminder_dismissed_at,
            )
            .await
        })
    }

    fn delete_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_event(principal_account_id, event_id)
                .await
        })
    }

    fn fetch_accessible_tasks_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        Box::pin(async move {
            self.fetch_client_tasks_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn fetch_mapi_notes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move { self.fetch_client_notes(account_id).await })
    }

    fn fetch_mapi_notes_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move { self.fetch_client_notes_by_ids(account_id, ids).await })
    }

    fn fetch_mapi_journal_entries<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move { self.fetch_journal_entries(account_id).await })
    }

    fn fetch_mapi_journal_entries_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move { self.fetch_journal_entries_by_ids(account_id, ids).await })
    }

    fn upsert_mapi_note<'a>(&'a self, input: UpsertClientNoteInput) -> StoreFuture<'a, ClientNote> {
        Box::pin(async move { self.upsert_client_note(input).await })
    }

    fn upsert_mapi_journal_entry<'a>(
        &'a self,
        input: UpsertJournalEntryInput,
    ) -> StoreFuture<'a, JournalEntry> {
        Box::pin(async move { self.upsert_journal_entry(input).await })
    }

    fn delete_mapi_note<'a>(&'a self, account_id: Uuid, note_id: Uuid) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_note(account_id, note_id).await })
    }

    fn delete_mapi_journal_entry<'a>(
        &'a self,
        account_id: Uuid,
        entry_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_journal_entry(account_id, entry_id).await })
    }

    fn fetch_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        Box::pin(async move { self.fetch_active_sieve_script(account_id).await })
    }

    fn list_mailbox_rules<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<MailboxRule>> {
        Box::pin(async move { self.list_mailbox_rules(account_id).await })
    }

    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        Box::pin(async move {
            self.put_sieve_script(account_id, name, content, activate, audit)
                .await
        })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { self.set_active_sieve_script(account_id, name, audit).await })
    }

    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_sieve_script(account_id, name, audit).await })
    }

    fn create_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        Box::pin(async move { self.upsert_client_task(input).await })
    }

    fn update_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
        mut input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        Box::pin(async move {
            input.id = Some(task_id);
            self.upsert_client_task(input).await
        })
    }

    fn update_accessible_task_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        reminder_reset: Option<bool>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.update_accessible_task_reminder(
                principal_account_id,
                task_id,
                reminder_set,
                reminder_at,
                reminder_dismissed_at,
                reminder_reset,
            )
            .await
        })
    }

    fn delete_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_task(principal_account_id, task_id).await })
    }

    };
}

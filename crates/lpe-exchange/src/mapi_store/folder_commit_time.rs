use super::*;

impl MapiMailStoreSnapshot {
    pub(crate) fn folder_local_commit_time_max(
        &self,
        folder_id: u64,
        mailboxes: &[JmapMailbox],
    ) -> Option<u64> {
        if matches!(
            folder_id,
            crate::mapi::identity::ROOT_FOLDER_ID
                | crate::mapi::identity::IPM_SUBTREE_FOLDER_ID
                | crate::mapi::identity::COMMON_VIEWS_FOLDER_ID
                | crate::mapi::identity::SEARCH_FOLDER_ID
                | crate::mapi::identity::REMINDERS_FOLDER_ID
                | crate::mapi::identity::SYNC_ISSUES_FOLDER_ID
                | crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
                | crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID
                | crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID
                | crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
                | crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
                | crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID
                | crate::mapi::identity::PUBLIC_FOLDERS_ROOT_FOLDER_ID
        ) || self.public_folder_for_id(folder_id).is_some()
            || self
                .search_folder_definition_for_folder_id(folder_id)
                .is_some()
        {
            return None;
        }

        let mut local_commit_time_max = 0;
        let mut observe = |value: u64| {
            local_commit_time_max = local_commit_time_max.max(value);
        };

        // [MS-OXCFOLD] section 2.2.2.2.1.14 and [MS-OXCFXICS] section
        // 3.1.5.3 define this as a top-level object modification time. Use the
        // independent PostgreSQL aggregate: selective and size-limited
        // snapshots do not necessarily contain every message in the folder.
        if let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) {
            if let Some(commit_time) = self
                .mailbox_content_commit_times
                .get(&folder.canonical_id)
                .copied()
            {
                observe(commit_time);
            } else if folder.mailbox.total_emails > 0 {
                return None;
            }
        }
        for event in self
            .events
            .iter()
            .filter(|event| event.folder_id == folder_id)
        {
            observe(crate::mapi_mailstore::filetime_from_rfc3339_utc(
                &event.version.updated_at,
            ));
        }
        for contact in self
            .contacts
            .iter()
            .filter(|contact| contact.folder_id == folder_id)
        {
            observe(*self.contact_commit_times.get(&contact.canonical_id)?);
        }
        for task in self.tasks.iter().filter(|task| task.folder_id == folder_id) {
            observe(crate::mapi_mailstore::filetime_from_rfc3339_utc(
                &task.task.updated_at,
            ));
        }
        for note in self.notes.iter().filter(|note| note.folder_id == folder_id) {
            observe(crate::mapi_mailstore::filetime_from_rfc3339_utc(
                &note.note.updated_at,
            ));
        }
        for entry in self
            .journal_entries
            .iter()
            .filter(|entry| entry.folder_id == folder_id)
        {
            observe(crate::mapi_mailstore::filetime_from_rfc3339_utc(
                &entry.entry.updated_at,
            ));
        }
        for config in self
            .associated_configs
            .iter()
            .filter(|config| config.folder_id == folder_id)
        {
            let commit_time = config
                .properties_json
                .get("__lpe_updated_at")
                .and_then(serde_json::Value::as_str)
                .map(crate::mapi_mailstore::filetime_from_rfc3339_utc)
                .filter(|commit_time| *commit_time != 0)?;
            observe(commit_time);
        }
        for child in self.folders.iter().filter(|child| {
            child.id != folder_id
                && crate::mapi_mailstore::mapi_folder_parent_id_for_mailbox(
                    &child.mailbox,
                    mailboxes,
                ) == folder_id
        }) {
            if let Some(version) = self.folder_version(child.id) {
                observe(version.last_modification_time);
            }
        }

        (local_commit_time_max != 0).then_some(local_commit_time_max)
    }
}

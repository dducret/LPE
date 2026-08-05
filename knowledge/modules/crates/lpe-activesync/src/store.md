---
type: Rust Module
title: store
resource: crates/lpe-activesync/src/store.rs#L1-L659
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-activesyncattachment-activesyncattachmentcontent-activesyncdevicestate-activesyncitemstate-activesyncsyncstate-auditentryinput-canonicalchangelistener-clientcontact-clientevent-jmapemail-jmapemailfollowupupdate-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-jmapuploadblob-mailboxaccountaccess-saveddraftmessage-storage-submitmessageinput-submittedmessage-upsertclientcontactinput-upsertclienteventinput
  - external/std-future-future-pin-pin
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [ActiveSyncStore](../../../../interfaces/crates/lpe-activesync/src/store/ActiveSyncStore.md)
- [fetch_accessible_mailbox_accounts](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_accessible_mailbox_accounts.md)
- [fetch_jmap_mailboxes](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_jmap_mailboxes.md)
- [create_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/destroy_jmap_mailbox.md)
- [query_jmap_email_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/query_jmap_email_ids.md)
- [fetch_jmap_emails](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_jmap_emails.md)
- [fetch_latest_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_latest_activesync_sync_state.md)
- [fetch_activesync_device](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_device.md)
- [store_activesync_device_pending_policy](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/store_activesync_device_pending_policy.md)
- [acknowledge_activesync_device_policy](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/acknowledge_activesync_device_policy.md)
- [touch_activesync_device](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/touch_activesync_device.md)
- [create_canonical_change_listener](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/create_canonical_change_listener.md)
- [fetch_activesync_email_states](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_email_states.md)
- [fetch_activesync_email_states_by_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_email_states_by_ids.md)
- [fetch_jmap_draft](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_jmap_draft.md)
- [fetch_jmap_message_blob](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_jmap_message_blob.md)
- [move_jmap_email_from_mailbox](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/move_jmap_email_from_mailbox.md)
- [delete_jmap_email_from_mailbox](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/delete_jmap_email_from_mailbox.md)
- [update_jmap_email_flags](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/update_jmap_email_flags.md)
- [update_jmap_email_followup_flags](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/update_jmap_email_followup_flags.md)
- [fetch_activesync_message_attachments](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_message_attachments.md)
- [fetch_activesync_attachment_content](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_attachment_content.md)
- [save_draft_message](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/save_draft_message.md)
- [delete_draft_message](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/delete_draft_message.md)
- [submit_message](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/submit_message.md)
- [fetch_client_contacts](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_client_contacts.md)
- [fetch_client_contacts_by_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_client_contacts_by_ids.md)
- [upsert_client_contact](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/upsert_client_contact.md)
- [delete_client_contact](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/delete_client_contact.md)
- [fetch_client_events](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_client_events.md)
- [fetch_client_events_by_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_client_events_by_ids.md)
- [upsert_client_event](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/upsert_client_event.md)
- [delete_client_event](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/delete_client_event.md)
- [fetch_activesync_contact_states](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_contact_states.md)
- [fetch_activesync_contact_states_by_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_contact_states_by_ids.md)
- [fetch_activesync_event_states](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_event_states.md)
- [fetch_activesync_event_states_by_ids](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_event_states_by_ids.md)
- [store_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/store_activesync_sync_state.md)
- [fetch_activesync_sync_state](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/fetch_activesync_sync_state.md)
- [cleanup_expired_activesync_sync_cursors](../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/cleanup_expired_activesync_sync_cursors.md)

# Imports

- `anyhow::Result`
- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{
    ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncDeviceState, ActiveSyncItemState,
    ActiveSyncSyncState, AuditEntryInput, CanonicalChangeListener, ClientContact, ClientEvent,
    JmapEmail, JmapEmailFollowupUpdate, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JmapUploadBlob, MailboxAccountAccess, SavedDraftMessage, Storage,
    SubmitMessageInput, SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput,
}`
- `std::{future::Future, pin::Pin}`
- `uuid::Uuid`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)
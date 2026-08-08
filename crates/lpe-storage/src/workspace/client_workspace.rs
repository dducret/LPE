use anyhow::Result;
use uuid::Uuid;

use crate::{
    attachments, AccessibleContact, AccessibleEvent, ClientAttachment, ClientAttachmentRow,
    ClientMessageRow, Storage,
};

use super::{ClientMailbox, ClientMessage, ClientWorkspace};

impl Storage {
    pub async fn fetch_client_workspace(
        &self,
        principal_account_id: Uuid,
        account_id: Uuid,
    ) -> Result<ClientWorkspace> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let message_rows = sqlx::query_as::<_, ClientMessageRow>(
            r#"
            SELECT
                m.id,
                mb.id AS mailbox_id,
                mb.role AS mailbox_role,
                COALESCE(NULLIF(fr.display_name, ''), fr.address, '') AS from_name,
                COALESCE(fr.address, '') AS from_address,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.tenant_id = m.tenant_id
                      AND r.message_id = m.id
                      AND r.role = 'to'
                ), '') AS to_recipients,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.tenant_id = m.tenant_id
                      AND r.message_id = m.id
                      AND r.role = 'cc'
                ), '') AS cc_recipients,
                m.normalized_subject AS subject,
                COALESCE(left(b.body_text, 160), '') AS preview,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS received_at,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'HH24:MI') AS time_label,
                NOT mm.is_seen AS unread,
                mm.is_flagged AS flagged,
                mm.followup_flag_status,
                CASE
                    WHEN mm.followup_start_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS followup_start_at,
                CASE
                    WHEN mm.followup_due_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS followup_due_at,
                CASE
                    WHEN mm.followup_completed_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS followup_completed_at,
                mm.reminder_set,
                CASE
                    WHEN mm.reminder_at IS NULL THEN NULL
                    ELSE to_char(mm.reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS reminder_at,
                CASE
                    WHEN mm.reminder_dismissed_at IS NULL THEN NULL
                    ELSE to_char(mm.reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS reminder_dismissed_at,
                COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END) AS delivery_status,
                COALESCE(b.body_text, '') AS body_text
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.visibility <> 'expunged'
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            WHERE m.tenant_id = $1
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC
            LIMIT 250
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let mailboxes = self
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .map(ClientMailbox::from)
            .collect();

        let attachment_rows = sqlx::query_as::<_, ClientAttachmentRow>(
            r#"
            SELECT
                a.id,
                a.message_id,
                a.file_name AS name,
                COALESCE(mp.content_type, '') AS media_type,
                a.size_octets
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.message_id = a.message_id
             AND mm.account_id = $2
             AND mm.visibility <> 'expunged'
            LEFT JOIN mime_parts mp
              ON mp.tenant_id = a.tenant_id
             AND mp.message_id = a.message_id
             AND mp.id = a.mime_part_id
            WHERE a.tenant_id = $1
            ORDER BY a.file_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let accessible_events = self.fetch_accessible_events(principal_account_id).await?;
        let event_collection_ids = accessible_events
            .iter()
            .map(|event| (event.id.to_string(), event.collection_id.clone()))
            .collect();
        let events = accessible_events
            .into_iter()
            .map(client_event_from_accessible)
            .collect();
        let contacts = self
            .fetch_accessible_contacts(principal_account_id)
            .await?
            .into_iter()
            .map(client_contact_from_accessible)
            .collect();
        let contact_books = self
            .fetch_accessible_contact_collections(principal_account_id)
            .await?;
        let calendar_collections = self
            .fetch_accessible_calendar_collections(principal_account_id)
            .await?;
        let tasks = self.fetch_client_tasks(account_id).await?;

        let messages = message_rows
            .into_iter()
            .map(|row| {
                let attachments = attachment_rows
                    .iter()
                    .filter(|attachment| attachment.message_id == row.id)
                    .map(|attachment| ClientAttachment {
                        id: attachment.id,
                        name: attachment.name.clone(),
                        kind: attachments::attachment_kind(
                            &attachment.media_type,
                            &attachment.name,
                        ),
                        size: format_size(attachment.size_octets),
                    })
                    .collect();

                ClientMessage {
                    id: row.id,
                    folder: client_folder(&row.mailbox_role, row.mailbox_id),
                    from: row.from_name,
                    from_address: row.from_address,
                    to: row.to_recipients,
                    cc: row.cc_recipients,
                    subject: row.subject,
                    preview: row.preview,
                    received_at: row.received_at,
                    time_label: row.time_label,
                    unread: row.unread,
                    flagged: row.flagged,
                    followup_flag_status: row.followup_flag_status,
                    followup_start_at: row.followup_start_at,
                    followup_due_at: row.followup_due_at,
                    followup_completed_at: row.followup_completed_at,
                    reminder_set: row.reminder_set,
                    reminder_at: row.reminder_at,
                    reminder_dismissed_at: row.reminder_dismissed_at,
                    tags: client_message_tags(&row.mailbox_role, &row.delivery_status),
                    attachments,
                    body: body_paragraphs(&row.body_text),
                }
            })
            .collect();

        Ok(ClientWorkspace {
            messages,
            mailboxes,
            events,
            event_collection_ids,
            contacts,
            contact_books,
            calendar_collections,
            tasks,
        })
    }
}

fn body_paragraphs(body_text: &str) -> Vec<String> {
    let paragraphs = body_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if paragraphs.is_empty() {
        vec!["".to_string()]
    } else {
        paragraphs
    }
}

fn client_folder(role: &str, mailbox_id: Uuid) -> String {
    match role {
        "drafts" => "drafts",
        "sent" => "sent",
        "archive" => "archive",
        "trash" => "trash",
        "junk" => "junk",
        "outbox" => "outbox",
        "rss_feeds" => "rss_feeds",
        "conversation_history" => "conversation_history",
        "sync_issues" => "sync_issues",
        "conflicts" => "conflicts",
        "local_failures" => "local_failures",
        "server_failures" => "server_failures",
        _ => return format!("mailbox:{mailbox_id}"),
    }
    .to_string()
}

fn client_message_tags(role: &str, delivery_status: &str) -> Vec<String> {
    if role == "drafts" || delivery_status == "draft" {
        return vec!["Draft".to_string()];
    }
    if role == "sent" {
        return vec!["Outgoing".to_string()];
    }
    Vec::new()
}

fn format_size(size_octets: i64) -> String {
    let size = size_octets.max(0) as f64;
    if size >= 1_048_576.0 {
        format!("{:.1} MB", size / 1_048_576.0)
    } else if size >= 1024.0 {
        format!("{:.0} KB", size / 1024.0)
    } else {
        format!("{} B", size as i64)
    }
}

fn client_event_from_accessible(event: AccessibleEvent) -> super::ClientEvent {
    super::ClientEvent {
        id: event.id,
        uid: event.uid,
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        all_day: event.all_day,
        status: event.status,
        sequence: event.sequence,
        recurrence_rule: event.recurrence_rule,
        recurrence_json: event.recurrence_json,
        recurrence_exceptions_json: event.recurrence_exceptions_json,
        title: event.title,
        location: event.location,
        organizer_json: event.organizer_json,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
        body_html: event.body_html,
    }
}

fn client_contact_from_accessible(contact: AccessibleContact) -> super::ClientContact {
    super::ClientContact {
        id: contact.id,
        address_book_id: contact.collection_id,
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
        structured_name: contact.structured_name,
        emails_json: contact.emails_json,
        phones_json: contact.phones_json,
        addresses_json: contact.addresses_json,
        urls_json: contact.urls_json,
        organization_name: contact.organization_name,
        job_title: contact.job_title,
        raw_vcard: contact.raw_vcard,
        source: contact.source,
    }
}

#[cfg(test)]
mod tests {
    use super::client_folder;
    use uuid::Uuid;

    #[test]
    fn client_folder_keeps_custom_mailboxes_distinct() {
        let mailbox_id = Uuid::from_u128(1);

        assert_eq!(
            client_folder("custom", mailbox_id),
            "mailbox:00000000-0000-0000-0000-000000000001"
        );
    }
}

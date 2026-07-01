use super::super::*;

#[derive(Debug, Clone)]
pub(in crate::service) struct ParsedReminderItemId {
    pub(in crate::service) source_type: String,
    pub(in crate::service) source_id: Uuid,
    pub(in crate::service) occurrence_start_at: Option<String>,
}

pub(in crate::service) fn get_reminders_response(reminders: &[ClientReminder]) -> String {
    let mut reminders_xml = String::new();
    for reminder in reminders {
        let reminder_id = reminder_item_id(reminder);
        reminders_xml.push_str(&format!(
            concat!(
                "<t:Reminder>",
                "<t:Subject>{title}</t:Subject>",
                "<t:Location/>",
                "<t:ReminderTime>{reminder_at}</t:ReminderTime>",
                "<t:StartDate>{start_at}</t:StartDate>",
                "<t:EndDate>{due_at}</t:EndDate>",
                "<t:ItemId Id=\"{id}\" ChangeKey=\"{status}\"/>",
                "</t:Reminder>"
            ),
            title = escape_xml(&reminder.title),
            reminder_at = escape_xml(&reminder.reminder_at),
            start_at = escape_xml(
                reminder
                    .occurrence_start_at
                    .as_deref()
                    .or(reminder.due_at.as_deref())
                    .unwrap_or(&reminder.reminder_at)
            ),
            due_at = escape_xml(reminder.due_at.as_deref().unwrap_or(&reminder.reminder_at)),
            id = escape_xml(&reminder_id),
            status = escape_xml(&reminder.status),
        ));
    }
    format!(
        concat!(
            "<m:GetRemindersResponse>",
            "<m:ResponseMessages>",
            "<m:GetRemindersResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Reminders>{reminders_xml}</m:Reminders>",
            "</m:GetRemindersResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetRemindersResponse>"
        ),
        reminders_xml = reminders_xml
    )
}

fn reminder_item_id(reminder: &ClientReminder) -> String {
    if let Some(occurrence_start_at) = reminder.occurrence_start_at.as_deref() {
        format!(
            "{}:{}:{}",
            reminder.source_type, reminder.source_id, occurrence_start_at
        )
    } else {
        format!("{}:{}", reminder.source_type, reminder.source_id)
    }
}

pub(in crate::service) fn parse_reminder_item_id(id: &str) -> Option<ParsedReminderItemId> {
    let mut parts = id.splitn(3, ':');
    let source_type = parts.next()?.to_ascii_lowercase();
    let source_id = Uuid::parse_str(parts.next()?).ok()?;
    let occurrence_start_at = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    Some(ParsedReminderItemId {
        source_type,
        source_id,
        occurrence_start_at,
    })
}

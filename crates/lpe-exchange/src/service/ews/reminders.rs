use super::super::*;

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

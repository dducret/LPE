use super::*;

pub(in crate::mapi) fn split_reminder_property_values(
    values: Vec<(u32, MapiValue)>,
) -> Result<(HashMap<u32, MapiValue>, Option<bool>, Option<String>)> {
    let mut properties = HashMap::new();
    let mut reminder_set = None;
    let mut reminder_time = None;
    let mut reminder_signal_time = None;
    for (tag, value) in values {
        match canonical_property_storage_tag(tag) {
            PID_LID_REMINDER_SET_TAG => {
                reminder_set = Some(
                    value
                        .as_bool()
                        .ok_or_else(|| anyhow!("invalid PidLidReminderSet value"))?,
                );
            }
            PID_LID_REMINDER_TIME_TAG => {
                reminder_time = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid reminder time value"))?,
                );
            }
            PID_LID_REMINDER_SIGNAL_TIME_TAG => {
                reminder_signal_time = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid reminder signal time value"))?,
                );
            }
            _ => {
                properties.insert(tag, value);
            }
        }
    }
    Ok((
        properties,
        reminder_set,
        reminder_signal_time.or(reminder_time),
    ))
}

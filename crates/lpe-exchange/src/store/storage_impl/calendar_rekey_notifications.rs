fn mapi_calendar_rekey_notification_ids_from_change_row(
    row: &sqlx::postgres::PgRow,
    notification_account_id: uuid::Uuid,
) -> Option<(u64, u64)> {
    use sqlx::Row;

    if row.try_get::<String, _>("object_kind").ok()?.as_str() != "calendar_event" {
        return None;
    }
    let summary = row
        .try_get::<serde_json::Value, _>("summary_json")
        .ok()?;
    let identity_account_id = summary
        .get("mapiIdentityAccountId")?
        .as_str()
        .and_then(|value| uuid::Uuid::parse_str(value).ok())?;
    if identity_account_id != notification_account_id {
        return None;
    }
    let old_message_id = summary.get("oldMapiObjectId")?.as_u64()?;
    let new_message_id = summary.get("newMapiObjectId")?.as_u64()?;
    (old_message_id != 0 && old_message_id != new_message_id)
        .then_some((old_message_id, new_message_id))
}

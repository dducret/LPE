macro_rules! store_impl_mapi_calendar_properties {
    () => {
        fn fetch_mapi_calendar_property_values<'a>(
            &'a self,
            principal_account_id: Uuid,
            event_ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<MapiCalendarPropertyValue>> {
            Box::pin(async move {
                if event_ids.is_empty() {
                    return Ok(Vec::new());
                }
                let tenant_id = mapi_tenant_id_for_account(self, principal_account_id).await?;
                let standard_property_tags = MAPI_CALENDAR_STANDARD_PASSTHROUGH_PROPERTY_TAGS
                    .iter()
                    .copied()
                    .map(i64::from)
                    .collect::<Vec<_>>();
                let rows = sqlx::query(
                    r#"
                    SELECT
                        event.id AS event_id,
                        value.property_tag,
                        value.property_type,
                        value.property_value
                    FROM calendar_events event
                    JOIN mapi_custom_property_values value
                      ON value.tenant_id = event.tenant_id
                     AND value.account_id = event.owner_account_id
                     AND value.object_kind = 'calendar_event'
                     AND value.canonical_id = event.id
                    WHERE event.tenant_id = $1
                      AND event.id = ANY($2)
                      AND event.projection_state = 'visible'
                      AND (
                            value.property_tag >= 2147483648
                            OR value.property_tag = ANY($4)
                      )
                      AND (
                            event.owner_account_id = $3
                            OR EXISTS (
                                SELECT 1
                                FROM calendar_grants grant_row
                                WHERE grant_row.tenant_id = event.tenant_id
                                  AND grant_row.owner_account_id = event.owner_account_id
                                  AND grant_row.calendar_id = event.calendar_id
                                  AND grant_row.grantee_account_id = $3
                                  AND grant_row.may_read
                            )
                      )
                    ORDER BY event.id, value.property_tag, value.property_type
                    "#,
                )
                .bind(tenant_id)
                .bind(event_ids)
                .bind(principal_account_id)
                .bind(&standard_property_tags)
                .fetch_all(self.pool())
                .await?;

                Ok(rows
                    .into_iter()
                    .map(|row| MapiCalendarPropertyValue {
                        event_id: row.get("event_id"),
                        property_tag: row.get::<i64, _>("property_tag") as u32,
                        property_type: row.get::<i32, _>("property_type") as u16,
                        property_value: row.get("property_value"),
                    })
                    .collect())
            })
        }

        fn fetch_mapi_calendar_recipient_response_times<'a>(
            &'a self,
            principal_account_id: Uuid,
            event_ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<MapiCalendarRecipientResponseTime>> {
            Box::pin(async move {
                if event_ids.is_empty() {
                    return Ok(Vec::new());
                }
                let tenant_id = mapi_tenant_id_for_account(self, principal_account_id).await?;
                let rows = sqlx::query(
                    r#"
                    SELECT
                        event.id AS event_id,
                        response_state.key AS attendee_email,
                        response_state.value->>'sentAt' AS response_sent_at
                    FROM calendar_events event
                    CROSS JOIN LATERAL jsonb_each(event.meeting_response_state_json)
                        AS response_state(key, value)
                    WHERE event.tenant_id = $1
                      AND event.id = ANY($2)
                      AND event.projection_state = 'visible'
                      AND jsonb_typeof(response_state.value) = 'object'
                      AND jsonb_typeof(response_state.value->'sentAt') = 'string'
                      AND (
                            event.owner_account_id = $3
                            OR EXISTS (
                                SELECT 1
                                FROM calendar_grants grant_row
                                WHERE grant_row.tenant_id = event.tenant_id
                                  AND grant_row.owner_account_id = event.owner_account_id
                                  AND grant_row.calendar_id = event.calendar_id
                                  AND grant_row.grantee_account_id = $3
                                  AND grant_row.may_read
                            )
                      )
                    ORDER BY event.id, response_state.key
                    "#,
                )
                .bind(tenant_id)
                .bind(event_ids)
                .bind(principal_account_id)
                .fetch_all(self.pool())
                .await?;

                Ok(rows
                    .into_iter()
                    .map(|row| MapiCalendarRecipientResponseTime {
                        event_id: row.get("event_id"),
                        attendee_email: row.get("attendee_email"),
                        response_sent_at: row.get("response_sent_at"),
                    })
                    .collect())
            })
        }
    };
}

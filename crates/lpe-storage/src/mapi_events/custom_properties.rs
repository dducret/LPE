use super::*;

pub(super) const PID_TAG_SEARCH_KEY: u32 = 0x300B_0102;

pub(super) fn mapi_event_search_key(values: &[MapiEventCustomPropertyValue]) -> Option<Vec<u8>> {
    values
        .iter()
        .find(|value| value.property_tag == PID_TAG_SEARCH_KEY)
        .and_then(|value| {
            (value.property_value.len() == 18 && value.property_value[..2] == [16, 0])
                .then(|| value.property_value[2..].to_vec())
        })
}

pub(super) async fn apply_mapi_event_custom_properties_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    event_id: Uuid,
    upserts: &[MapiEventCustomPropertyValue],
    deletes: &[u32],
    allow_initial_search_key: bool,
) -> Result<()> {
    // Preserve the valid pre-first-Save SearchKey observed in the Probe B/C/D
    // traces. [MS-OXCPRPT] section 2.2.1.9 defines its read-only/stable
    // semantics; [MS-OXCMSG] section 2.2 product note <1> records Exchange's
    // product exception, which LPE deliberately bounds to initial creation.
    let mut replaced_tags = deletes
        .iter()
        .copied()
        .chain(upserts.iter().map(|value| value.property_tag))
        .filter(|tag| *tag != PID_TAG_SEARCH_KEY)
        .collect::<Vec<_>>();
    replaced_tags.sort_unstable();
    replaced_tags.dedup();
    if !replaced_tags.is_empty() {
        let replaced_tags = replaced_tags.into_iter().map(i64::from).collect::<Vec<_>>();
        sqlx::query(
            r#"
            DELETE FROM mapi_custom_property_values
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'calendar_event'
              AND canonical_id = $3
              AND property_tag = ANY($4)
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(event_id)
        .bind(&replaced_tags)
        .execute(&mut **tx)
        .await?;
    }
    for value in upserts {
        if value.property_tag == PID_TAG_SEARCH_KEY && !allow_initial_search_key {
            continue;
        }
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type, property_value
            )
            VALUES ($1, $2, 'calendar_event', $3, $4, $5, $6)
            ON CONFLICT (tenant_id, account_id, object_kind, canonical_id, property_tag, property_type)
            DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(event_id)
        .bind(i64::from(value.property_tag))
        .bind(i32::from(value.property_type))
        .bind(&value.property_value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn fetch_mapi_event_search_key_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    event_id: Uuid,
) -> Result<Option<Vec<u8>>> {
    let value = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT property_value FROM mapi_custom_property_values \
         WHERE tenant_id = $1 AND account_id = $2 AND object_kind = 'calendar_event' \
           AND canonical_id = $3 AND property_tag = 806027522 AND property_type = 258",
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(event_id)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(value.and_then(|property_value| {
        mapi_event_search_key(&[MapiEventCustomPropertyValue {
            property_tag: PID_TAG_SEARCH_KEY,
            property_type: 0x0102,
            property_value,
        }])
    }))
}

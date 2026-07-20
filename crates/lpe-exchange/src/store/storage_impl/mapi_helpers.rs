fn ews_user_configuration_from_row(row: sqlx::postgres::PgRow) -> EwsUserConfiguration {
    EwsUserConfiguration {
        id: row.get("id"),
        scope_kind: row.get("scope_kind"),
        mailbox_id: row.get("mailbox_id"),
        public_folder_id: row.get("public_folder_id"),
        config_name: row.get("config_name"),
        config_class: row.get("config_class"),
        dictionary_json: row.get("dictionary_json"),
        xml_payload: row.get("xml_payload"),
        binary_payload: row.get("binary_payload"),
        modseq: row.get::<i64, _>("modseq") as u64,
    }
}

fn ews_scope_emails(principal: &AccountPrincipal, mailbox_emails: &[String]) -> Vec<String> {
    let mut emails = mailbox_emails
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if emails.is_empty() {
        emails.push(principal.email.trim().to_ascii_lowercase());
    }
    emails.sort();
    emails.dedup();
    emails
}

fn parse_message_uuid(item_id: &str) -> Option<Uuid> {
    let value = item_id
        .trim()
        .strip_prefix("message:")
        .unwrap_or_else(|| item_id.trim());
    Uuid::parse_str(value).ok()
}

fn split_ews_recipient_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

async fn mapi_special_object_kind_for_checkpoint_mailbox(
    storage: &Storage,
    tenant_id: &Uuid,
    account_id: Uuid,
    checkpoint_kind: MapiCheckpointKind,
    mailbox_id: Option<Uuid>,
) -> Result<Option<&'static str>> {
    if checkpoint_kind == MapiCheckpointKind::Hierarchy {
        return Ok(None);
    }
    let Some(mailbox_id) = mailbox_id else {
        return Ok(None);
    };
    let matches_virtual_folder = |folder_id| {
        crate::mapi_mailstore::virtual_special_mailbox(folder_id)
            .map(|mailbox| mailbox.id == mailbox_id)
            .unwrap_or(false)
    };
    if [
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
    ]
    .into_iter()
    .any(matches_virtual_folder)
    {
        return Ok(Some("contact"));
    }
    if matches_virtual_folder(crate::mapi::identity::CALENDAR_FOLDER_ID) {
        return Ok(Some("calendar_event"));
    }
    if matches_virtual_folder(crate::mapi::identity::TRASH_FOLDER_ID) {
        return Ok(Some("deleted_calendar_event"));
    }
    if [
        crate::mapi::identity::TASKS_FOLDER_ID,
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
        crate::mapi::identity::REMINDERS_FOLDER_ID,
    ]
    .into_iter()
    .any(matches_virtual_folder)
    {
        return Ok(Some("task"));
    }
    if matches_virtual_folder(crate::mapi::identity::NOTES_FOLDER_ID) {
        return Ok(Some("note"));
    }
    if matches_virtual_folder(crate::mapi::identity::JOURNAL_FOLDER_ID) {
        return Ok(Some("journal_entry"));
    }
    if matches_virtual_folder(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID) {
        return Ok(Some("conversation_action"));
    }
    if matches_virtual_folder(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID) {
        return Ok(Some("navigation_shortcut"));
    }
    let mailbox_role = sqlx::query_scalar::<_, String>(
        r#"
        SELECT role
        FROM mailboxes
        WHERE tenant_id = $1
          AND account_id = $2
          AND id = $3
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(mailbox_id)
    .fetch_optional(storage.pool())
    .await?;
    Ok((mailbox_role.as_deref() == Some("trash")).then_some("deleted_calendar_event"))
}

async fn advance_mapi_replica_counter_past_allocated(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let next_counter = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT GREATEST(
            COALESCE(MAX(mapi_global_counter), $3 - 1) + 1,
            $3
        )
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND mapi_global_counter < $4
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .bind(crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_one(&mut **tx)
    .await?;

    // Equality with FIRST_RESERVED_HIGH_GLOBAL_COUNTER is the exhausted
    // high-watermark and must not recycle unmaterialized client reservations.
    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = CASE
                WHEN next_global_counter < $3 OR next_global_counter > $4 THEN $3
                ELSE GREATEST(next_global_counter, $3)
            END,
            updated_at = CASE
                WHEN next_global_counter < $3 OR next_global_counter > $4 THEN NOW()
                ELSE updated_at
            END
        WHERE tenant_id = $1
          AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(next_counter)
    .bind(crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn repair_reserved_mapi_identity_counter_collisions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT identities.object_kind, identities.canonical_id, identities.mapi_global_counter,
               mailboxes.role
        FROM mapi_object_identities identities
        LEFT JOIN mailboxes
          ON mailboxes.tenant_id = identities.tenant_id
         AND mailboxes.account_id = identities.account_id
         AND mailboxes.id = identities.canonical_id
         AND identities.object_kind = 'mailbox'
        WHERE identities.tenant_id = $1
          AND identities.account_id = $2
          AND identities.mapi_global_counter >= $3
          AND identities.mapi_global_counter < $4
        ORDER BY identities.mapi_global_counter, identities.created_at, identities.canonical_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::JOURNAL_FOLDER_COUNTER as i64)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let object_kind = row.get::<String, _>("object_kind");
        let role = row.try_get::<String, _>("role").ok();
        let current_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        let canonical_id = row.get::<Uuid, _>("canonical_id");
        if object_kind == "mailbox"
            && (role
                .as_deref()
                .and_then(crate::mapi_store::reserved_folder_counter_for_role)
                == Some(current_counter)
                || canonical_id
                    == crate::mapi_mailstore::virtual_special_mailbox_id(
                        crate::mapi::identity::mapi_store_id(current_counter),
                    ))
        {
            continue;
        }

        let global_counter = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
        let (object_id, source_key, change_key, instance_key) =
            crate::mapi::identity::persisted_identity_material(global_counter);

        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                change_key = $8,
                instance_key = $9
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $3
              AND canonical_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(object_kind)
        .bind(canonical_id)
        .bind(global_counter as i64)
        .bind(object_id as i64)
        .bind(source_key)
        .bind(change_key)
        .bind(instance_key)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn repair_reserved_mapi_mailbox_identities(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT identities.canonical_id, identities.mapi_global_counter, mailboxes.role
        FROM mapi_object_identities identities
        JOIN mailboxes
          ON mailboxes.tenant_id = identities.tenant_id
         AND mailboxes.account_id = identities.account_id
         AND mailboxes.id = identities.canonical_id
        WHERE identities.tenant_id = $1
          AND identities.account_id = $2
          AND identities.object_kind = 'mailbox'
          AND identities.deleted_at IS NULL
        ORDER BY identities.created_at, identities.canonical_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let role = row.get::<String, _>("role");
        let Some(reserved_counter) = crate::mapi_store::reserved_folder_counter_for_role(&role)
        else {
            continue;
        };
        let current_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        if current_counter == reserved_counter {
            continue;
        }

        let canonical_id = row.get::<Uuid, _>("canonical_id");
        let (object_id, source_key, change_key, instance_key) =
            crate::mapi::identity::persisted_identity_material(reserved_counter);

        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                change_key = $8,
                instance_key = $9
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'mailbox'
              AND canonical_id = $3
              AND mapi_global_counter = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(canonical_id)
        .bind(current_counter as i64)
        .bind(reserved_counter as i64)
        .bind(object_id as i64)
        .bind(source_key)
        .bind(change_key)
        .bind(instance_key)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn allocate_next_mapi_global_counter(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<u64> {
    let next = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
        RETURNING next_global_counter - 1
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await?;

    if next as u64 >= crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER {
        anyhow::bail!("MAPI dynamic global counter space exhausted");
    }

    Ok(next as u64)
}

async fn fetch_mapi_named_property_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    property: &MapiNamedProperty,
) -> Result<Option<MapiNamedPropertyMapping>> {
    let (property_kind, property_lid, property_name) = mapi_named_property_parts(property);
    let row = sqlx::query(
        r#"
        SELECT property_id, property_guid, property_kind, property_lid, property_name
        FROM mapi_named_properties
        WHERE tenant_id = $1
          AND account_id = $2
          AND property_guid = $3
          AND property_kind = $4
          AND (
              ($4 = 'lid' AND property_lid = $5)
              OR ($4 = 'name' AND property_name = $6)
          )
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(property.guid.to_vec())
    .bind(property_kind)
    .bind(property_lid)
    .bind(property_name)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(mapi_named_property_mapping_from_row).transpose()
}

async fn insert_mapi_named_property_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    property_id: u16,
    property: &MapiNamedProperty,
) -> Result<()> {
    let (property_kind, property_lid, property_name) = mapi_named_property_parts(property);
    sqlx::query(
        r#"
        INSERT INTO mapi_named_properties (
            tenant_id,
            account_id,
            property_id,
            property_guid,
            property_kind,
            property_lid,
            property_name
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(i32::from(property_id))
    .bind(property.guid.to_vec())
    .bind(property_kind)
    .bind(property_lid)
    .bind(property_name)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn allocate_next_mapi_named_property_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<u16> {
    let existing = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT property_id
        FROM mapi_named_properties
        WHERE tenant_id = $1
          AND account_id = $2
        ORDER BY property_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;
    let existing = existing
        .into_iter()
        .filter_map(|id| u16::try_from(id).ok())
        .collect::<std::collections::HashSet<_>>();
    for property_id in crate::mapi::properties::DYNAMIC_NAMED_PROPERTY_ID_START
        ..=crate::mapi::properties::MAX_NAMED_PROPERTY_ID
    {
        if existing.contains(&property_id) || is_reserved_named_property_id(property_id) {
            continue;
        }
        return Ok(property_id);
    }
    anyhow::bail!("MAPI named property id space exhausted");
}

fn mapi_named_property_parts(
    property: &MapiNamedProperty,
) -> (&'static str, Option<i32>, Option<&str>) {
    match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => ("lid", Some(*lid as i32), None),
        MapiNamedPropertyKind::Name(name) => ("name", None, Some(name.as_str())),
    }
}

const MAPI_PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

fn normalize_mapi_named_property(mut property: MapiNamedProperty) -> MapiNamedProperty {
    if property.guid == MAPI_PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}

fn is_unique_violation(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .and_then(|error| match error {
            sqlx::Error::Database(database_error) => database_error.code(),
            _ => None,
        })
        .as_deref()
        == Some("23505")
}

fn mapi_named_property_mapping_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiNamedPropertyMapping> {
    let guid: Vec<u8> = row.get("property_guid");
    let guid: [u8; 16] = guid
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid MAPI named property GUID length"))?;
    let property_kind: String = row.get("property_kind");
    let kind = match property_kind.as_str() {
        "lid" => MapiNamedPropertyKind::Lid(row.get::<i32, _>("property_lid") as u32),
        "name" => MapiNamedPropertyKind::Name(row.get::<String, _>("property_name")),
        value => anyhow::bail!("unsupported MAPI named property kind: {value}"),
    };
    Ok(MapiNamedPropertyMapping {
        property_id: row.get::<i32, _>("property_id") as u16,
        property: MapiNamedProperty { guid, kind },
    })
}

#[allow(dead_code)]
fn mapi_custom_property_value_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiCustomPropertyValue> {
    Ok(MapiCustomPropertyValue {
        property_tag: row.get::<i64, _>("property_tag") as u32,
        property_type: row.get::<i32, _>("property_type") as u16,
        property_value: row.get("property_value"),
    })
}

fn mapi_folder_profile_property_value_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiFolderProfilePropertyValue> {
    Ok(MapiFolderProfilePropertyValue {
        folder_id: row.get::<i64, _>("folder_id") as u64,
        property_tag: row.get::<i64, _>("property_tag") as u32,
        property_type: row.get::<i32, _>("property_type") as u16,
        property_value: row.get("property_value"),
    })
}

fn mapi_navigation_shortcut_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiNavigationShortcutRecord> {
    Ok(MapiNavigationShortcutRecord {
        id: row.try_get("id")?,
        account_id: row.try_get("account_id")?,
        subject: row.try_get("subject")?,
        target_folder_id: row
            .try_get::<Option<i64>, _>("target_folder_id")?
            .map(|value| value as u64),
        shortcut_type: row.try_get::<i64, _>("shortcut_type")? as u32,
        flags: row.try_get::<i64, _>("flags")? as u32,
        save_stamp: row.try_get::<i64, _>("save_stamp")? as u32,
        section: row.try_get::<i64, _>("section")? as u32,
        ordinal: row.try_get("ordinal")?,
        group_header_id: row.try_get("group_header_id")?,
        group_name: row.try_get("group_name")?,
        client_properties: MapiNavigationShortcutClientProperties {
            calendar_color: row.try_get("calendar_color")?,
            address_book_entry_id: row.try_get("address_book_entry_id")?,
            address_book_store_entry_id: row.try_get("address_book_store_entry_id")?,
            client_id: row.try_get("client_id")?,
            ro_group_type: row.try_get("ro_group_type")?,
        },
    })
}

fn mapi_associated_config_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiAssociatedConfigRecord> {
    let mut properties_json: serde_json::Value = row.try_get("properties_json")?;
    if let Ok(updated_at) = row.try_get::<String, _>("updated_at") {
        if let Some(properties) = properties_json.as_object_mut() {
            properties.insert(
                "__lpe_updated_at".to_string(),
                serde_json::json!(updated_at),
            );
        }
    }
    Ok(MapiAssociatedConfigRecord {
        id: row.try_get("id")?,
        account_id: row.try_get("account_id")?,
        folder_id: row.try_get::<i64, _>("folder_id")? as u64,
        message_class: row.try_get("message_class")?,
        subject: row.try_get("subject")?,
        properties_json,
    })
}

async fn insert_mapi_navigation_shortcut_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    shortcut_id: Uuid,
    change_kind: &str,
) -> Result<()> {
    let modseq = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(MAX(modseq), 0) + 1
        FROM mail_change_log
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind, modseq,
            affected_principal_ids, summary_json
        )
        VALUES ($1, $2, 'navigation_shortcut', $3, $4, $5, ARRAY[$2]::uuid[], '{}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(shortcut_id)
    .bind(change_kind)
    .bind(modseq)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_mapi_associated_config_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    config_id: Uuid,
    change_kind: &str,
    folder_id: u64,
) -> Result<()> {
    let modseq = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(MAX(modseq), 0) + 1
        FROM mail_change_log
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind, modseq,
            affected_principal_ids, summary_json
        )
        VALUES ($1, $2, 'associated_config', $3, $4, $5, ARRAY[$2]::uuid[], $6)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(config_id)
    .bind(change_kind)
    .bind(modseq)
    .bind(serde_json::json!({ "folderId": folder_id.to_string() }))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn repair_invalid_mapi_identity_material(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT object_kind, canonical_id, mapi_global_counter, mapi_change_number,
               source_key, change_key, predecessor_change_list, instance_key
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut repaired_count = 0u64;
    for row in rows {
        let global_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        let change_number = row.get::<i64, _>("mapi_change_number") as u64;
        let stored_source_key = row.get::<Vec<u8>, _>("source_key");
        let stored_change_key = row.get::<Vec<u8>, _>("change_key");
        let stored_predecessor_change_list =
            row.get::<Vec<u8>, _>("predecessor_change_list");
        let stored_instance_key = row.get::<Vec<u8>, _>("instance_key");
        let (_, source_key, object_change_key, instance_key) =
            crate::mapi::identity::persisted_identity_material(global_counter);
        let current_change_key =
            crate::mapi::identity::change_key_for_change_number(change_number);
        // [MS-OXCFXICS] 2.2.1.2.7 and 3.1.5.3: an imported ChangeKey can
        // intentionally differ from the internal CN. Repair only the exact
        // stale signature left by the former identity-material repair: the CK
        // fell back to the immutable object counter while the PCL proves that
        // the current local CN was already integrated.
        let change_key = if change_number != global_counter
            && stored_change_key == object_change_key
            && predecessor_change_list_contains_exact_xid(
                &stored_predecessor_change_list,
                &current_change_key,
            )
        {
            current_change_key
        } else {
            stored_change_key.clone()
        };
        if stored_source_key == source_key
            && stored_change_key == change_key
            && stored_instance_key == instance_key
        {
            continue;
        }
        let updated = sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET source_key = $5,
                change_key = $6,
                instance_key = $7
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $3
              AND canonical_id = $4
              AND mapi_change_number = $8
              AND source_key = $9
              AND change_key = $10
              AND predecessor_change_list = $11
              AND instance_key = $12
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(row.get::<String, _>("object_kind"))
        .bind(row.get::<Uuid, _>("canonical_id"))
        .bind(source_key)
        .bind(change_key)
        .bind(instance_key)
        .bind(change_number as i64)
        .bind(stored_source_key)
        .bind(stored_change_key)
        .bind(stored_predecessor_change_list)
        .bind(stored_instance_key)
        .execute(&mut **tx)
        .await?;
        repaired_count += updated.rows_affected();
    }

    if repaired_count > 0 {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            account_id = %account_id,
            repaired_invalid_identity_material_count = repaired_count,
            message = "rca debug mapi repaired invalid identity material",
        );
    }

    Ok(())
}

fn predecessor_change_list_contains_exact_xid(
    predecessor_change_list: &[u8],
    expected_xid: &[u8],
) -> bool {
    let mut offset = 0usize;
    let mut found = false;
    while offset < predecessor_change_list.len() {
        let size = usize::from(predecessor_change_list[offset]);
        offset += 1;
        if !(17..=24).contains(&size) {
            return false;
        }
        let Some(end) = offset.checked_add(size) else {
            return false;
        };
        let Some(xid) = predecessor_change_list.get(offset..end) else {
            return false;
        };
        if xid == expected_xid {
            found = true;
        }
        offset = end;
    }
    found
}

async fn mapi_collaboration_folder_identity_ids_for_account(
    storage: &Storage,
    account_id: Uuid,
) -> Result<Vec<Uuid>> {
    let contact_collections = storage
        .fetch_accessible_contact_collections(account_id)
        .await?;
    let calendar_collections = storage
        .fetch_accessible_calendar_collections(account_id)
        .await?;
    let task_collections = storage
        .fetch_accessible_task_collections(account_id)
        .await?;
    Ok(crate::mapi_store::collaboration_folder_identity_requests(
        &contact_collections,
        &calendar_collections,
        &task_collections,
    )
    .into_iter()
    .map(|request| request.canonical_id)
    .collect())
}

async fn repair_stale_mapi_object_identities(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    preserved_mailbox_identity_ids: &[Uuid],
) -> Result<()> {
    let mut preserved_checkpoint_mailbox_ids = preserved_mailbox_identity_ids.to_vec();
    preserved_checkpoint_mailbox_ids.extend(crate::mapi_mailstore::virtual_special_mailbox_ids());
    let contact_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'contact'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM contacts contact
              WHERE contact.tenant_id = identity.tenant_id
                AND contact.id = identity.canonical_id
                AND (
                    contact.owner_account_id = identity.account_id
                    OR EXISTS (
                        SELECT 1
                        FROM contact_book_grants grant_row
                        WHERE grant_row.tenant_id = contact.tenant_id
                          AND grant_row.owner_account_id = contact.owner_account_id
                          AND grant_row.contact_book_id = contact.contact_book_id
                          AND grant_row.grantee_account_id = identity.account_id
                          AND grant_row.may_read
                    )
                )
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let calendar_event_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'calendar_event'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM calendar_events event
              WHERE event.tenant_id = identity.tenant_id
                AND event.id = identity.canonical_id
                AND (
                    event.owner_account_id = identity.account_id
                    OR EXISTS (
                        SELECT 1
                        FROM calendar_grants grant_row
                        WHERE grant_row.tenant_id = event.tenant_id
                          AND grant_row.owner_account_id = event.owner_account_id
                          AND grant_row.calendar_id = event.calendar_id
                          AND grant_row.grantee_account_id = identity.account_id
                          AND grant_row.may_read
                    )
                )
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let task_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'task'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM tasks task
              WHERE task.tenant_id = identity.tenant_id
                AND task.owner_account_id = identity.account_id
                AND task.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let mailbox_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'mailbox'
          AND identity.deleted_at IS NULL
          AND identity.mapi_global_counter >= $3
          AND NOT (identity.canonical_id = ANY($4::uuid[]))
          AND NOT EXISTS (
              SELECT 1
              FROM mailboxes mailbox
              WHERE mailbox.tenant_id = identity.tenant_id
                AND mailbox.account_id = identity.account_id
                AND mailbox.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .bind(preserved_mailbox_identity_ids)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let search_folder_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'search_folder_definition'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM search_folders search_folder
              WHERE search_folder.tenant_id = identity.tenant_id
                AND search_folder.account_id = identity.account_id
                AND search_folder.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let orphaned_sync_checkpoint_count = sqlx::query(
        r#"
        DELETE FROM mapi_sync_checkpoints checkpoint
        WHERE checkpoint.tenant_id = $1
          AND checkpoint.account_id = $2
          AND checkpoint.mailbox_id IS NOT NULL
          AND NOT (checkpoint.mailbox_id = ANY($3::uuid[]))
          AND NOT EXISTS (
              SELECT 1
              FROM mailboxes mailbox
              WHERE mailbox.tenant_id = checkpoint.tenant_id
                AND mailbox.account_id = checkpoint.account_id
                AND mailbox.id = checkpoint.mailbox_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(&preserved_checkpoint_mailbox_ids)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let orphaned_associated_config_count = sqlx::query(
        r#"
        DELETE FROM mapi_associated_config_messages config
        WHERE config.tenant_id = $1
          AND config.account_id = $2
          AND NOT (
              config.folder_id = ANY($3::bigint[])
              OR EXISTS (
                  SELECT 1
                  FROM mapi_object_identities identity
                  WHERE identity.tenant_id = config.tenant_id
                    AND identity.account_id = config.account_id
                    AND identity.mapi_object_id = config.folder_id
                    AND identity.object_kind IN ('mailbox', 'search_folder_definition')
                    AND identity.deleted_at IS NULL
              )
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS.as_slice())
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let associated_config_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'associated_config'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM mapi_associated_config_messages config
              WHERE config.tenant_id = identity.tenant_id
                AND config.account_id = identity.account_id
                AND config.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let navigation_shortcut_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM mapi_navigation_shortcuts shortcut
              WHERE shortcut.tenant_id = identity.tenant_id
                AND shortcut.account_id = identity.account_id
                AND shortcut.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    let total_count = contact_count
        + calendar_event_count
        + task_count
        + mailbox_count
        + search_folder_count
        + associated_config_count
        + navigation_shortcut_count
        + orphaned_sync_checkpoint_count
        + orphaned_associated_config_count;
    if total_count > 0 {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            account_id = %account_id,
            repaired_stale_contact_identity_count = contact_count,
            repaired_stale_calendar_event_identity_count = calendar_event_count,
            repaired_stale_task_identity_count = task_count,
            repaired_stale_mailbox_identity_count = mailbox_count,
            repaired_stale_search_folder_identity_count = search_folder_count,
            repaired_stale_associated_config_identity_count = associated_config_count,
            repaired_stale_navigation_shortcut_identity_count = navigation_shortcut_count,
            repaired_orphaned_mapi_sync_checkpoint_count = orphaned_sync_checkpoint_count,
            repaired_orphaned_mapi_associated_config_count = orphaned_associated_config_count,
            repaired_stale_mapi_object_identity_count = total_count,
            message = "rca debug mapi repaired stale object identities",
        );
    }

    Ok(())
}

fn mapi_content_table_order_by(sort_orders: &[MapiContentTableSort]) -> String {
    if sort_orders.is_empty() {
        return "received_at DESC, id DESC".to_string();
    }

    let mut clauses = sort_orders
        .iter()
        .map(|sort| {
            let column = match sort.field {
                MapiContentTableSortField::ReceivedAt => "received_at",
                MapiContentTableSortField::ClientSubmitTime => "client_submit_time_key",
                MapiContentTableSortField::Subject => "subject_key",
                MapiContentTableSortField::SenderName => "sender_name_key",
                MapiContentTableSortField::SenderEmail => "sender_email_key",
                MapiContentTableSortField::DisplayTo => "display_to_key",
                MapiContentTableSortField::MessageSize => "size_octets",
                MapiContentTableSortField::HasAttachments => "has_attachments",
                MapiContentTableSortField::MessageFlags => "message_flags",
            };
            let direction = if sort.descending { "DESC" } else { "ASC" };
            format!("{column} {direction}")
        })
        .collect::<Vec<_>>();
    clauses.push("id DESC".to_string());
    clauses.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapi_content_table_order_by_uses_projected_columns() {
        let sort_fields = [
            MapiContentTableSortField::ReceivedAt,
            MapiContentTableSortField::ClientSubmitTime,
            MapiContentTableSortField::Subject,
            MapiContentTableSortField::SenderName,
            MapiContentTableSortField::SenderEmail,
            MapiContentTableSortField::DisplayTo,
            MapiContentTableSortField::MessageSize,
            MapiContentTableSortField::HasAttachments,
            MapiContentTableSortField::MessageFlags,
        ];
        let sort_orders = sort_fields
            .into_iter()
            .map(|field| MapiContentTableSort {
                field,
                descending: false,
            })
            .collect::<Vec<_>>();

        let order_by = mapi_content_table_order_by(&sort_orders);

        assert!(!order_by.contains("mm."));
        assert!(!order_by.contains("m."));
        assert!(order_by.contains("client_submit_time_key ASC"));
        assert!(order_by.contains("message_flags ASC"));
        assert!(order_by.ends_with("id DESC"));
    }

    #[test]
    fn associated_config_cleanup_keeps_virtual_parent_folders() {
        let allowed = MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS;

        assert!(allowed.contains(&(crate::mapi::identity::CONTACTS_FOLDER_ID as i64)));
        assert!(allowed.contains(&(crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID as i64)));
        assert!(allowed.contains(&(crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID as i64)));
        assert!(allowed.contains(&(crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID as i64)));
        assert!(allowed.contains(&(crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID as i64)));
        assert!(allowed.contains(&(crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID as i64)));
    }
}


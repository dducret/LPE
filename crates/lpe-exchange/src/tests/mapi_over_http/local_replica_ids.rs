use super::*;

// [MS-OXCFXICS] sections 2.2.3.2.4.7 and 3.3.5.2.1 require the server to
// reserve the complete ID range returned to a local replica for its exclusive use.
#[tokio::test]
async fn mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon_response.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let initial_next_global_counter = 0x0000_0002_0000u64;
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET next_global_counter = EXCLUDED.next_global_counter
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
    .bind(initial_next_global_counter as i64)
    .execute(storage.pool())
    .await?;

    let requested_id_count = 0x0001_0000u32;
    let mut rops = vec![
        0x7F, 0x00, 0x00, // RopGetLocalReplicaIds
    ];
    rops.extend_from_slice(&requested_id_count.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await?;
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(response_rops[0], 0x7F);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into()?),
        0,
        "RopGetLocalReplicaIds failed"
    );
    assert_eq!(
        &response_rops[6..22],
        &crate::mapi::identity::STORE_REPLICA_GUID
    );
    let first_global_counter =
        crate::mapi::identity::global_counter_from_globcnt(response_rops[22..28].try_into()?)
            .ok_or_else(|| anyhow::anyhow!("missing first local-replica global counter"))?;
    let expected_next_global_counter = first_global_counter + u64::from(requested_id_count);
    let persisted_next_global_counter = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT next_global_counter
        FROM mapi_mailbox_replicas
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await? as u64;

    assert!(first_global_counter >= initial_next_global_counter);
    assert_eq!(persisted_next_global_counter, expected_next_global_counter);

    let following_identity = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::AssociatedConfig,
                canonical_id: Uuid::parse_str("21000000-0000-0000-0000-000000000010")?,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?
        .remove(0);
    let following_global_counter =
        crate::mapi::identity::global_counter_from_store_id(following_identity.object_id)
            .ok_or_else(|| anyhow::anyhow!("invalid following MAPI object id"))?;
    assert!(following_global_counter >= expected_next_global_counter);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_local_replica_id_reservations_are_atomic_in_postgresql() -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let id_count = crate::store::MAX_MAPI_LOCAL_REPLICA_ID_COUNT;

    let (first, second) = tokio::join!(
        storage.reserve_mapi_local_replica_ids(account_id, id_count),
        storage.reserve_mapi_local_replica_ids(account_id, id_count),
    );
    let mut starts = [first?, second?];
    starts.sort_unstable();
    assert_eq!(starts[1], starts[0] + u64::from(id_count));

    let persisted_next_global_counter = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT next_global_counter
        FROM mapi_mailbox_replicas
        WHERE account_id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await? as u64;
    assert_eq!(
        persisted_next_global_counter,
        starts[1] + u64::from(id_count)
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_local_replica_exhaustion_does_not_recycle_reserved_ranges_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = sqlx::query_scalar::<_, Uuid>("SELECT tenant_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(storage.pool())
        .await?;
    let id_count = crate::store::MAX_MAPI_LOCAL_REPLICA_ID_COUNT;
    let exhausted_counter = crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER;
    let first_counter = exhausted_counter - u64::from(id_count);

    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET next_global_counter = EXCLUDED.next_global_counter
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
    .bind(first_counter as i64)
    .execute(storage.pool())
    .await?;

    assert_eq!(
        storage
            .reserve_mapi_local_replica_ids(account_id, id_count)
            .await?,
        first_counter
    );
    storage
        .fetch_or_allocate_mapi_identities(account_id, &[])
        .await?;
    let persisted_next_global_counter = sqlx::query_scalar::<_, i64>(
        "SELECT next_global_counter FROM mapi_mailbox_replicas WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await? as u64;
    assert_eq!(persisted_next_global_counter, exhausted_counter);

    fixture.cleanup().await?;
    Ok(())
}

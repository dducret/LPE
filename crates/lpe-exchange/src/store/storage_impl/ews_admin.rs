macro_rules! store_impl_ews_admin {
    () => {
    fn fetch_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
    ) -> StoreFuture<'a, Option<EwsUserConfiguration>> {
        Box::pin(async move {
            let row = sqlx::query(
                r#"
                SELECT id, scope_kind, mailbox_id, public_folder_id, config_name, config_class,
                       dictionary_json, xml_payload, binary_payload, modseq
                FROM account_client_configurations
                WHERE account_id = $1
                  AND scope_kind = $2
                  AND mailbox_id IS NOT DISTINCT FROM $3
                  AND public_folder_id IS NOT DISTINCT FROM $4
                  AND config_name = $5
                  AND config_class = $6
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .bind(&key.scope_kind)
            .bind(key.mailbox_id)
            .bind(key.public_folder_id)
            .bind(&key.config_name)
            .bind(&key.config_class)
            .fetch_optional(self.pool())
            .await?;

            Ok(row.map(ews_user_configuration_from_row))
        })
    }

    fn upsert_ews_user_configuration<'a>(
        &'a self,
        input: UpsertEwsUserConfigurationInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsUserConfiguration> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(input.account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let payload_size = input
                .xml_payload
                .as_ref()
                .map(|value| value.len())
                .unwrap_or(0)
                + input
                    .binary_payload
                    .as_ref()
                    .map(|value| value.len())
                    .unwrap_or(0);
            let existing_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM account_client_configurations
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND scope_kind = $3
                  AND mailbox_id IS NOT DISTINCT FROM $4
                  AND public_folder_id IS NOT DISTINCT FROM $5
                  AND config_name = $6
                  AND config_class = $7
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(input.account_id)
            .bind(&input.key.scope_kind)
            .bind(input.key.mailbox_id)
            .bind(input.key.public_folder_id)
            .bind(&input.key.config_name)
            .bind(&input.key.config_class)
            .fetch_optional(self.pool())
            .await?;

            let row = if let Some(existing_id) = existing_id {
                sqlx::query(
                    r#"
                    UPDATE account_client_configurations
                    SET dictionary_json = $2,
                        xml_payload = $3,
                        binary_payload = $4,
                        payload_size_octets = $5,
                        modseq = modseq + 1,
                        updated_at = NOW()
                    WHERE id = $1
                    RETURNING id, scope_kind, mailbox_id, public_folder_id, config_name,
                              config_class, dictionary_json, xml_payload, binary_payload, modseq
                    "#,
                )
                .bind(existing_id)
                .bind(&input.dictionary_json)
                .bind(&input.xml_payload)
                .bind(&input.binary_payload)
                .bind(payload_size as i32)
                .fetch_one(self.pool())
                .await?
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO account_client_configurations (
                        id, tenant_id, account_id, scope_kind, mailbox_id, public_folder_id,
                        config_name, config_class, dictionary_json, xml_payload, binary_payload,
                        payload_size_octets
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    RETURNING id, scope_kind, mailbox_id, public_folder_id, config_name,
                              config_class, dictionary_json, xml_payload, binary_payload, modseq
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(tenant_id)
                .bind(input.account_id)
                .bind(&input.key.scope_kind)
                .bind(input.key.mailbox_id)
                .bind(input.key.public_folder_id)
                .bind(&input.key.config_name)
                .bind(&input.key.config_class)
                .bind(&input.dictionary_json)
                .bind(&input.xml_payload)
                .bind(&input.binary_payload)
                .bind(payload_size as i32)
                .fetch_one(self.pool())
                .await?
            };
            self.append_audit_event(tenant_id, audit).await?;
            Ok(ews_user_configuration_from_row(row))
        })
    }

    fn delete_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let result = sqlx::query(
                r#"
                DELETE FROM account_client_configurations
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND scope_kind = $3
                  AND mailbox_id IS NOT DISTINCT FROM $4
                  AND public_folder_id IS NOT DISTINCT FROM $5
                  AND config_name = $6
                  AND config_class = $7
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(&key.scope_kind)
            .bind(key.mailbox_id)
            .bind(key.public_folder_id)
            .bind(&key.config_name)
            .bind(&key.config_class)
            .execute(self.pool())
            .await?;
            if result.rows_affected() > 0 {
                self.append_audit_event(tenant_id, audit).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    fn fetch_ews_retention_policy_tags<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsRetentionPolicyTag>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                WITH assignment AS (
                    SELECT default_tag_id
                    FROM account_retention_policy_assignments
                    WHERE tenant_id = $1
                      AND account_id = $2
                )
                SELECT
                    tag.id,
                    tag.display_name,
                    tag.tag_type,
                    tag.action,
                    tag.retention_days,
                    tag.is_visible,
                    tag.description,
                    COALESCE(tag.id = assignment.default_tag_id, FALSE) AS opted_into
                FROM retention_policy_tags tag
                LEFT JOIN assignment ON TRUE
                WHERE tag.tenant_id = $1
                  AND tag.lifecycle_state = 'active'
                  AND (tag.is_visible OR tag.id = assignment.default_tag_id)
                ORDER BY
                    CASE WHEN tag.id = assignment.default_tag_id THEN 0 ELSE 1 END,
                    lower(tag.display_name),
                    tag.id
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsRetentionPolicyTag {
                        id: row.try_get("id")?,
                        display_name: row.try_get("display_name")?,
                        tag_type: row.try_get("tag_type")?,
                        action: row.try_get("action")?,
                        retention_days: row.try_get("retention_days")?,
                        is_visible: row.try_get("is_visible")?,
                        description: row.try_get("description")?,
                        opted_into: row.try_get("opted_into")?,
                    })
                })
                .collect()
        })
    }

    fn create_managed_retention_folder<'a>(
        &'a self,
        input: ManagedRetentionFolderCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.create_managed_retention_folder(input, audit).await })
    }

    fn fetch_ews_searchable_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsSearchableMailbox>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT id, primary_email, display_name, litigation_hold_enabled
                FROM accounts
                WHERE tenant_id = $1
                ORDER BY lower(primary_email), id
                "#,
            )
            .bind(principal.tenant_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsSearchableMailbox {
                        account_id: row.try_get("id")?,
                        email: row.try_get("primary_email")?,
                        display_name: row.try_get("display_name")?,
                        litigation_hold_enabled: row.try_get("litigation_hold_enabled")?,
                    })
                })
                .collect()
        })
    }

    fn fetch_ews_discovery_search_configurations<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsDiscoverySearchConfig>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT id, display_name, query_text,
                       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                FROM discovery_searches
                WHERE tenant_id = $1
                ORDER BY updated_at DESC, id
                LIMIT 100
                "#,
            )
            .bind(principal.tenant_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsDiscoverySearchConfig {
                        id: row.try_get("id")?,
                        display_name: row.try_get("display_name")?,
                        query_text: row.try_get("query_text")?,
                        updated_at: row.try_get("updated_at")?,
                    })
                })
                .collect()
        })
    }

    fn fetch_ews_message_tracking_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        limit: usize,
    ) -> StoreFuture<'a, Vec<EwsMessageTrackingReport>> {
        Box::pin(async move {
            let query = query_text.trim().to_ascii_lowercase();
            let like_query = format!("%{query}%");
            let rows = sqlx::query(
                r#"
                SELECT
                    q.id,
                    q.account_id,
                    COALESCE(q.sender_address, q.from_address) AS sender,
                    COALESCE(m.normalized_subject, '') AS subject,
                    q.status,
                    q.last_trace_id,
                    q.remote_message_ref,
                    to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS submitted_at,
                    COALESCE(string_agg(DISTINCT r.address, ',' ORDER BY r.address), '') AS recipients
                FROM submission_queue q
                JOIN mailbox_messages smm
                  ON smm.tenant_id = q.tenant_id
                 AND smm.account_id = q.account_id
                 AND smm.id = q.sent_mailbox_message_id
                JOIN messages m
                  ON m.tenant_id = q.tenant_id
                 AND m.id = smm.message_id
                LEFT JOIN submission_recipients r
                  ON r.tenant_id = q.tenant_id
                 AND r.submission_queue_id = q.id
                 AND r.protected_metadata = FALSE
                WHERE q.tenant_id = $1
                  AND q.account_id = $2
                  AND (
                    $3::text = ''
                    OR lower(COALESCE(m.normalized_subject, '')) LIKE $4
                    OR lower(COALESCE(q.sender_address, q.from_address)) LIKE $4
                    OR lower(COALESCE(q.last_trace_id, '')) LIKE $4
                    OR lower(COALESCE(q.remote_message_ref, '')) LIKE $4
                    OR lower(COALESCE(r.address, '')) LIKE $4
                  )
                GROUP BY q.id, q.account_id, q.sender_address, q.from_address, m.normalized_subject,
                         q.status, q.last_trace_id, q.remote_message_ref, q.created_at
                ORDER BY q.created_at DESC, q.id
                LIMIT $5
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(&query)
            .bind(&like_query)
            .bind(limit.max(1).min(100) as i64)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    let id: Uuid = row.try_get("id")?;
                    let recipients: String = row.try_get("recipients")?;
                    Ok(EwsMessageTrackingReport {
                        report_id: id.to_string(),
                        account_id: row.try_get("account_id")?,
                        sender: row.try_get("sender")?,
                        recipients: split_ews_recipient_list(&recipients),
                        subject: row.try_get("subject")?,
                        submitted_at: row.try_get("submitted_at")?,
                        status: row.try_get("status")?,
                        trace_id: row.try_get("last_trace_id")?,
                        remote_message_ref: row.try_get("remote_message_ref")?,
                    })
                })
                .collect()
        })
    }

    fn fetch_ews_message_tracking_report_detail<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        report_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsMessageTrackingReportDetail>> {
        Box::pin(async move {
            let report_id = report_id.trim();
            if report_id.is_empty() {
                return Ok(None);
            }
            let row = sqlx::query(
                r#"
                SELECT
                    q.id,
                    q.account_id,
                    COALESCE(q.sender_address, q.from_address) AS sender,
                    COALESCE(m.normalized_subject, '') AS subject,
                    q.status,
                    q.last_trace_id,
                    q.remote_message_ref,
                    to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS submitted_at,
                    COALESCE(string_agg(DISTINCT r.address, ',' ORDER BY r.address), '') AS recipients
                FROM submission_queue q
                JOIN mailbox_messages smm
                  ON smm.tenant_id = q.tenant_id
                 AND smm.account_id = q.account_id
                 AND smm.id = q.sent_mailbox_message_id
                JOIN messages m
                  ON m.tenant_id = q.tenant_id
                 AND m.id = smm.message_id
                LEFT JOIN submission_recipients r
                  ON r.tenant_id = q.tenant_id
                 AND r.submission_queue_id = q.id
                 AND r.protected_metadata = FALSE
                WHERE q.tenant_id = $1
                  AND q.account_id = $2
                  AND (q.id::text = $3 OR q.last_trace_id = $3)
                GROUP BY q.id, q.account_id, q.sender_address, q.from_address, m.normalized_subject,
                         q.status, q.last_trace_id, q.remote_message_ref, q.created_at
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(report_id)
            .fetch_optional(self.pool())
            .await?;

            let Some(row) = row else {
                return Ok(None);
            };
            let queue_id: Uuid = row.try_get("id")?;
            let recipients: String = row.try_get("recipients")?;
            let report = EwsMessageTrackingReport {
                report_id: queue_id.to_string(),
                account_id: row.try_get("account_id")?,
                sender: row.try_get("sender")?,
                recipients: split_ews_recipient_list(&recipients),
                subject: row.try_get("subject")?,
                submitted_at: row.try_get("submitted_at")?,
                status: row.try_get("status")?,
                trace_id: row.try_get("last_trace_id")?,
                remote_message_ref: row.try_get("remote_message_ref")?,
            };

            let event_rows = sqlx::query(
                r#"
                SELECT event_source, event_kind, recipient_address,
                       to_char(event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS event_at,
                       dsn_json::text AS dsn_json
                FROM (
                    SELECT
                        'lpe'::text AS event_source,
                        e.event_kind,
                        NULL::text AS recipient_address,
                        e.received_at AS event_at,
                        e.dsn_json
                    FROM submission_events e
                    WHERE e.tenant_id = $1
                      AND e.submission_queue_id = $2
                    UNION ALL
                    SELECT
                        t.event_source,
                        t.event_kind,
                        t.recipient_address,
                        t.occurred_at AS event_at,
                        t.dsn_json
                    FROM lpe_ct_transport_trace_events t
                    WHERE t.tenant_id = $1
                      AND (
                        t.submission_queue_id = $2
                        OR ($3::text IS NOT NULL AND t.trace_id = $3)
                      )
                ) events
                ORDER BY event_at ASC, event_source ASC, event_kind ASC
                LIMIT 200
                "#,
            )
            .bind(principal.tenant_id)
            .bind(queue_id)
            .bind(report.trace_id.as_deref())
            .fetch_all(self.pool())
            .await?;
            let events = event_rows
                .into_iter()
                .map(|row| {
                    Ok(EwsMessageTrackingEvent {
                        event_source: row.try_get("event_source")?,
                        event_kind: row.try_get("event_kind")?,
                        recipient_address: row.try_get("recipient_address")?,
                        timestamp: row.try_get("event_at")?,
                        dsn_json: row.try_get("dsn_json")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Some(EwsMessageTrackingReportDetail { report, events }))
        })
    }

    fn search_ews_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        limit: usize,
    ) -> StoreFuture<'a, EwsDiscoverySearchResult> {
        Box::pin(async move {
            let scoped_emails = ews_scope_emails(principal, mailbox_emails);
            let search_id = Uuid::new_v4();
            let job_id = Uuid::new_v4();
            let mut tx = self.pool().begin().await?;
            sqlx::query(
                r#"
                INSERT INTO discovery_searches (
                    id, tenant_id, display_name, query_text, scope_json, created_by_account_id
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(search_id)
            .bind(principal.tenant_id)
            .bind("EWS SearchMailboxes")
            .bind(query_text.trim())
            .bind(serde_json::json!({ "mailboxes": scoped_emails }))
            .bind(principal.account_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO discovery_search_jobs (id, tenant_id, search_id, status, started_at)
                VALUES ($1, $2, $3, 'running', NOW())
                "#,
            )
            .bind(job_id)
            .bind(principal.tenant_id)
            .bind(search_id)
            .execute(&mut *tx)
            .await?;

            let rows = sqlx::query(
                r#"
                SELECT
                    a.id AS account_id,
                    s.mailbox_message_id,
                    s.message_id,
                    s.subject_text,
                    left(COALESCE(NULLIF(s.body_text, ''), s.attachment_text), 160) AS preview
                FROM mail_search_documents s
                JOIN accounts a
                  ON a.tenant_id = s.tenant_id
                 AND a.id = s.account_id
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                WHERE s.tenant_id = $1
                  AND lower(a.primary_email) = ANY($2)
                  AND mm.visibility = 'visible'
                  AND (
                    $3::text = ''
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                ORDER BY s.updated_at DESC, s.message_id DESC
                LIMIT $4
                "#,
            )
            .bind(principal.tenant_id)
            .bind(
                scoped_emails
                    .iter()
                    .map(|value| value.to_ascii_lowercase())
                    .collect::<Vec<_>>(),
            )
            .bind(query_text.trim())
            .bind(limit.max(1).min(100) as i64)
            .fetch_all(&mut *tx)
            .await?;

            let mut items = Vec::new();
            for (index, row) in rows.into_iter().enumerate() {
                let item = EwsDiscoverySearchItem {
                    id: Uuid::new_v4(),
                    account_id: row.try_get("account_id")?,
                    mailbox_message_id: row.try_get("mailbox_message_id")?,
                    message_id: row.try_get("message_id")?,
                    subject: row.try_get("subject_text")?,
                    preview: row.try_get("preview")?,
                    rank: index as i32,
                };
                sqlx::query(
                    r#"
                    INSERT INTO discovery_result_items (
                        id, tenant_id, job_id, account_id, mailbox_message_id, message_id, rank, preview
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    "#,
                )
                .bind(item.id)
                .bind(principal.tenant_id)
                .bind(job_id)
                .bind(item.account_id)
                .bind(item.mailbox_message_id)
                .bind(item.message_id)
                .bind(item.rank)
                .bind(&item.preview)
                .execute(&mut *tx)
                .await?;
                items.push(item);
            }
            sqlx::query(
                r#"
                UPDATE discovery_search_jobs
                SET status = 'completed',
                    result_count = $3,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE tenant_id = $1 AND id = $2
                "#,
            )
            .bind(principal.tenant_id)
            .bind(job_id)
            .bind(items.len() as i32)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            Ok(EwsDiscoverySearchResult {
                search_id,
                job_id,
                query_text: query_text.trim().to_string(),
                result_count: items.len(),
                items,
            })
        })
    }

    fn fetch_ews_mail_app_manifests<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsMailAppManifest>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT
                    c.id,
                    c.app_id,
                    c.display_name,
                    c.manifest_xml,
                    c.provider_name,
                    c.version,
                    i.status AS installation_status
                FROM mail_app_catalog c
                LEFT JOIN LATERAL (
                    SELECT status
                    FROM mail_app_installations i
                    WHERE i.tenant_id = c.tenant_id
                      AND i.app_catalog_id = c.id
                      AND i.status <> 'uninstalled'
                      AND (
                        i.install_scope = 'tenant'
                        OR (i.install_scope = 'account' AND i.account_id = $2)
                      )
                    ORDER BY CASE WHEN i.install_scope = 'account' THEN 0 ELSE 1 END, i.updated_at DESC
                    LIMIT 1
                ) i ON TRUE
                LEFT JOIN mail_app_tenant_policies p
                  ON p.tenant_id = c.tenant_id
                WHERE c.tenant_id = $1
                  AND c.lifecycle_state = 'active'
                  AND (
                    i.status IS NOT NULL
                    OR COALESCE(p.default_install_allowed, FALSE)
                  )
                ORDER BY lower(c.display_name), c.id
                LIMIT 100
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsMailAppManifest {
                        catalog_id: row.try_get("id")?,
                        app_id: row.try_get("app_id")?,
                        display_name: row.try_get("display_name")?,
                        manifest_xml: row.try_get("manifest_xml")?,
                        provider_name: row.try_get("provider_name")?,
                        version: row.try_get("version")?,
                        installation_status: row.try_get("installation_status")?,
                    })
                })
                .collect()
        })
    }

    fn fetch_ews_app_marketplace_policy<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsAppMarketplacePolicy> {
        Box::pin(async move {
            let row = sqlx::query(
                r#"
                SELECT marketplace_enabled, marketplace_url
                FROM mail_app_tenant_policies
                WHERE tenant_id = $1
                "#,
            )
            .bind(principal.tenant_id)
            .fetch_optional(self.pool())
            .await?;

            Ok(match row {
                Some(row) => EwsAppMarketplacePolicy {
                    enabled: row.try_get("marketplace_enabled")?,
                    url: row.try_get("marketplace_url")?,
                },
                None => EwsAppMarketplacePolicy {
                    enabled: false,
                    url: None,
                },
            })
        })
    }

    fn install_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        Box::pin(async move {
            let catalog_id = ews_mail_app_catalog_id(self, principal, app_id).await?;
            let allowed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT COALESCE(default_install_allowed, FALSE)
                FROM mail_app_tenant_policies
                WHERE tenant_id = $1
                "#,
            )
            .bind(principal.tenant_id)
            .fetch_optional(self.pool())
            .await?
            .unwrap_or(false);
            if !allowed {
                anyhow::bail!("mail app install access is not granted by tenant policy");
            }

            let row = sqlx::query(
                r#"
                INSERT INTO mail_app_installations (
                    id, tenant_id, app_catalog_id, account_id, install_scope, status,
                    installed_by_account_id
                )
                VALUES ($1, $2, $3, $4, 'account', 'installed', $4)
                ON CONFLICT (tenant_id, account_id, app_catalog_id)
                    WHERE install_scope = 'account' AND status <> 'uninstalled'
                DO UPDATE SET status = 'installed', updated_at = NOW()
                RETURNING app_catalog_id, status
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(principal.tenant_id)
            .bind(catalog_id)
            .bind(principal.account_id)
            .fetch_one(self.pool())
            .await?;
            sqlx::query(
                r#"
                INSERT INTO mail_app_consents (
                    id, tenant_id, app_catalog_id, account_id, consent_scope, granted_by_account_id
                )
                VALUES ($1, $2, $3, $4, 'ews', $4)
                ON CONFLICT (tenant_id, app_catalog_id, account_id, consent_scope)
                DO UPDATE SET revoked_at = NULL, granted_at = NOW(), granted_by_account_id = $4
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(principal.tenant_id)
            .bind(catalog_id)
            .bind(principal.account_id)
            .execute(self.pool())
            .await?;
            self.append_audit_event(principal.tenant_id, audit).await?;
            Ok(EwsMailAppInstall {
                catalog_id: row.try_get("app_catalog_id")?,
                app_id: app_id.trim().to_string(),
                status: row.try_get("status")?,
            })
        })
    }

    fn disable_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        Box::pin(async move {
            ews_update_mail_app_install_status(self, principal, app_id, "disabled", audit).await
        })
    }

    fn uninstall_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        Box::pin(async move {
            let install =
                ews_update_mail_app_install_status(self, principal, app_id, "uninstalled", audit)
                    .await?;
            sqlx::query(
                r#"
                UPDATE mail_app_token_events
                SET revoked_at = COALESCE(revoked_at, NOW())
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND app_catalog_id = $3
                  AND revoked_at IS NULL
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(install.catalog_id)
            .execute(self.pool())
            .await?;
            Ok(install)
        })
    }

    fn issue_ews_mail_app_token<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        token_hash: &'a str,
        scopes: &'a [String],
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppTokenEvent> {
        Box::pin(async move {
            let catalog_id = ews_mail_app_catalog_id(self, principal, app_id).await?;
            let installed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM mail_app_installations
                    WHERE tenant_id = $1
                      AND app_catalog_id = $2
                      AND status = 'installed'
                      AND (
                        install_scope = 'tenant'
                        OR (install_scope = 'account' AND account_id = $3)
                      )
                )
                "#,
            )
            .bind(principal.tenant_id)
            .bind(catalog_id)
            .bind(principal.account_id)
            .fetch_one(self.pool())
            .await?;
            if !installed {
                anyhow::bail!("mail app token access is not granted for an installed app");
            }

            let active_scopes = sqlx::query_scalar::<_, String>(
                r#"
                SELECT consent_scope
                FROM mail_app_consents
                WHERE tenant_id = $1
                  AND app_catalog_id = $2
                  AND account_id = $3
                  AND revoked_at IS NULL
                "#,
            )
            .bind(principal.tenant_id)
            .bind(catalog_id)
            .bind(principal.account_id)
            .fetch_all(self.pool())
            .await?;
            if !scopes
                .iter()
                .all(|scope| active_scopes.iter().any(|active| active == scope))
            {
                anyhow::bail!("mail app token scope access is not granted");
            }

            let row = sqlx::query(
                r#"
                INSERT INTO mail_app_token_events (
                    id, tenant_id, app_catalog_id, account_id, token_hash, scopes_json, expires_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, NOW() + INTERVAL '5 minutes')
                RETURNING id, app_catalog_id,
                          to_char(issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS issued_at,
                          to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(principal.tenant_id)
            .bind(catalog_id)
            .bind(principal.account_id)
            .bind(token_hash)
            .bind(serde_json::json!(scopes))
            .fetch_one(self.pool())
            .await?;
            self.append_audit_event(principal.tenant_id, audit).await?;
            Ok(EwsMailAppTokenEvent {
                id: row.try_get("id")?,
                catalog_id: row.try_get("app_catalog_id")?,
                app_id: app_id.trim().to_string(),
                issued_at: row.try_get("issued_at")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
    }

    fn create_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        phone_number: Option<&'a str>,
        message_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsUnifiedMessagingCall> {
        Box::pin(async move {
            let id = Uuid::new_v4();
            let call_id = format!("ews-{}", Uuid::new_v4());
            let row = sqlx::query(
                r#"
                INSERT INTO unified_messaging_calls (
                    id, tenant_id, account_id, call_id, call_kind, status, phone_number, message_id
                )
                VALUES ($1, $2, $3, $4, 'play_on_phone', 'requested', $5, $6)
                RETURNING id, call_id, call_kind, status, phone_number, message_id,
                          to_char(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
                          to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                "#,
            )
            .bind(id)
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(&call_id)
            .bind(phone_number)
            .bind(message_id)
            .fetch_one(self.pool())
            .await?;
            self.append_audit_event(principal.tenant_id, audit).await?;
            Ok(ews_unified_messaging_call_from_row(row)?)
        })
    }

    fn fetch_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>> {
        Box::pin(async move {
            let row = sqlx::query(ews_unified_messaging_call_select_sql())
                .bind(principal.tenant_id)
                .bind(principal.account_id)
                .bind(call_id.trim())
                .fetch_optional(self.pool())
                .await?;
            row.map(ews_unified_messaging_call_from_row).transpose()
        })
    }

    fn disconnect_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>> {
        Box::pin(async move {
            let row = sqlx::query(
                r#"
                UPDATE unified_messaging_calls
                SET status = 'cancelled',
                    completed_at = COALESCE(completed_at, NOW()),
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND call_id = $3
                  AND status IN ('requested', 'ringing', 'connected')
                RETURNING id, call_id, call_kind, status, phone_number, message_id,
                          to_char(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
                          to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(call_id.trim())
            .fetch_optional(self.pool())
            .await?;
            if row.is_some() {
                self.append_audit_event(principal.tenant_id, audit).await?;
            }
            row.map(ews_unified_messaging_call_from_row).transpose()
        })
    }

    fn fetch_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        mailbox_emails: &'a [String],
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>> {
        Box::pin(async move {
            let scoped_emails = ews_scope_emails(principal, mailbox_emails);
            let rows = sqlx::query(
                r#"
                SELECT
                    a.id AS account_id,
                    a.primary_email,
                    a.display_name,
                    h.id AS hold_id,
                    h.display_name AS hold_name,
                    h.query_text,
                    (h.id IS NOT NULL AND hm.released_at IS NULL AND h.status = 'active') AS active
                FROM accounts a
                LEFT JOIN compliance_hold_mailboxes hm
                  ON hm.tenant_id = a.tenant_id
                 AND hm.account_id = a.id
                 AND hm.released_at IS NULL
                LEFT JOIN compliance_holds h
                  ON h.tenant_id = hm.tenant_id
                 AND h.id = hm.hold_id
                 AND h.status = 'active'
                WHERE a.tenant_id = $1
                  AND lower(a.primary_email) = ANY($2)
                ORDER BY lower(a.primary_email), h.updated_at DESC NULLS LAST, h.id
                "#,
            )
            .bind(principal.tenant_id)
            .bind(
                scoped_emails
                    .iter()
                    .map(|value| value.to_ascii_lowercase())
                    .collect::<Vec<_>>(),
            )
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsHoldMailbox {
                        account_id: row.try_get("account_id")?,
                        email: row.try_get("primary_email")?,
                        display_name: row.try_get("display_name")?,
                        hold_id: row.try_get("hold_id")?,
                        hold_name: row.try_get("hold_name")?,
                        query_text: row.try_get("query_text")?,
                        active: row.try_get("active")?,
                    })
                })
                .collect()
        })
    }

    fn set_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        hold_name: &'a str,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        enable: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>> {
        Box::pin(async move {
            let scoped_emails = ews_scope_emails(principal, mailbox_emails);
            let normalized_hold_name = hold_name.trim();
            let normalized_hold_name = if normalized_hold_name.is_empty() {
                "EWS Litigation Hold"
            } else {
                normalized_hold_name
            };
            let mut tx = self.pool().begin().await?;
            let account_rows = sqlx::query(
                r#"
                SELECT id
                FROM accounts
                WHERE tenant_id = $1
                  AND lower(primary_email) = ANY($2)
                ORDER BY lower(primary_email), id
                "#,
            )
            .bind(principal.tenant_id)
            .bind(
                scoped_emails
                    .iter()
                    .map(|value| value.to_ascii_lowercase())
                    .collect::<Vec<_>>(),
            )
            .fetch_all(&mut *tx)
            .await?;
            let account_ids = account_rows
                .into_iter()
                .map(|row| row.try_get("id"))
                .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

            if enable {
                let hold_id = Uuid::new_v4();
                sqlx::query(
                    r#"
                    INSERT INTO compliance_holds (
                        id, tenant_id, display_name, query_text, status, created_by_account_id
                    )
                    VALUES ($1, $2, $3, $4, 'active', $5)
                    "#,
                )
                .bind(hold_id)
                .bind(principal.tenant_id)
                .bind(normalized_hold_name)
                .bind(query_text.trim())
                .bind(principal.account_id)
                .execute(&mut *tx)
                .await?;
                for account_id in &account_ids {
                    sqlx::query(
                        r#"
                        INSERT INTO compliance_hold_mailboxes (
                            tenant_id, hold_id, account_id, applied_by_account_id
                        )
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (tenant_id, hold_id, account_id) DO NOTHING
                        "#,
                    )
                    .bind(principal.tenant_id)
                    .bind(hold_id)
                    .bind(account_id)
                    .bind(principal.account_id)
                    .execute(&mut *tx)
                    .await?;
                }
                sqlx::query(
                    r#"
                    UPDATE accounts
                    SET litigation_hold_enabled = TRUE,
                        litigation_hold_started_at = COALESCE(litigation_hold_started_at, NOW())
                    WHERE tenant_id = $1 AND id = ANY($2)
                    "#,
                )
                .bind(principal.tenant_id)
                .bind(&account_ids)
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(
                    r#"
                    UPDATE compliance_hold_mailboxes hm
                    SET released_at = NOW()
                    FROM compliance_holds h
                    WHERE hm.tenant_id = h.tenant_id
                      AND hm.hold_id = h.id
                      AND hm.tenant_id = $1
                      AND hm.account_id = ANY($2)
                      AND h.display_name = $3
                      AND hm.released_at IS NULL
                    "#,
                )
                .bind(principal.tenant_id)
                .bind(&account_ids)
                .bind(normalized_hold_name)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    r#"
                    UPDATE compliance_holds
                    SET status = 'released',
                        released_by_account_id = $2,
                        released_at = NOW(),
                        updated_at = NOW()
                    WHERE tenant_id = $1
                      AND display_name = $3
                      AND status = 'active'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM compliance_hold_mailboxes hm
                        WHERE hm.tenant_id = compliance_holds.tenant_id
                          AND hm.hold_id = compliance_holds.id
                          AND hm.released_at IS NULL
                      )
                    "#,
                )
                .bind(principal.tenant_id)
                .bind(principal.account_id)
                .bind(normalized_hold_name)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    r#"
                    UPDATE accounts a
                    SET litigation_hold_enabled = FALSE,
                        litigation_hold_started_at = NULL
                    WHERE a.tenant_id = $1
                      AND a.id = ANY($2)
                      AND NOT EXISTS (
                        SELECT 1
                        FROM compliance_hold_mailboxes hm
                        JOIN compliance_holds h
                          ON h.tenant_id = hm.tenant_id
                         AND h.id = hm.hold_id
                        WHERE hm.tenant_id = a.tenant_id
                          AND hm.account_id = a.id
                          AND hm.released_at IS NULL
                          AND h.status = 'active'
                      )
                    "#,
                )
                .bind(principal.tenant_id)
                .bind(&account_ids)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            self.append_audit_event(principal.tenant_id, audit).await?;
            self.fetch_ews_hold_mailboxes(principal, &scoped_emails)
                .await
        })
    }

    fn fetch_ews_non_indexable_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsNonIndexableReport>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT
                    r.id,
                    r.account_id,
                    a.primary_email,
                    r.report_kind,
                    r.reason,
                    r.message_id,
                    r.attachment_id,
                    to_char(r.detected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS detected_at,
                    r.resolved_at IS NOT NULL AS resolved
                FROM non_indexable_item_reports r
                JOIN accounts a
                  ON a.tenant_id = r.tenant_id
                 AND a.id = r.account_id
                WHERE r.tenant_id = $1
                ORDER BY r.detected_at DESC, r.id
                LIMIT 200
                "#,
            )
            .bind(principal.tenant_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(EwsNonIndexableReport {
                        id: row.try_get("id")?,
                        account_id: row.try_get("account_id")?,
                        email: row.try_get("primary_email")?,
                        report_kind: row.try_get("report_kind")?,
                        reason: row.try_get("reason")?,
                        message_id: row.try_get("message_id")?,
                        attachment_id: row.try_get("attachment_id")?,
                        detected_at: row.try_get("detected_at")?,
                        resolved: row.try_get("resolved")?,
                    })
                })
                .collect()
        })
    }

    fn create_ews_transfer_job<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        direction: &'a str,
        item_ids: &'a [String],
        request_json: serde_json::Value,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsTransferJob> {
        Box::pin(async move {
            let job_id = Uuid::new_v4();
            let mut tx = self.pool().begin().await?;
            sqlx::query(
                r#"
                INSERT INTO mailbox_item_transfer_jobs (
                    id, tenant_id, account_id, direction, source_protocol, status,
                    requested_by_account_id, request_json, total_items
                )
                VALUES ($1, $2, $3, $4, 'ews', 'requested', $5, $6, $7)
                "#,
            )
            .bind(job_id)
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .bind(direction)
            .bind(principal.account_id)
            .bind(&request_json)
            .bind(item_ids.len() as i32)
            .execute(&mut *tx)
            .await?;

            let mut entries = Vec::new();
            for (ordinal, item_id) in item_ids.iter().enumerate() {
                let canonical_id = parse_message_uuid(item_id);
                let entry = EwsTransferEntry {
                    id: Uuid::new_v4(),
                    ordinal: ordinal as i32,
                    item_kind: "message".to_string(),
                    canonical_id,
                    source_item_id: Some(item_id.clone()),
                    status: "pending".to_string(),
                };
                sqlx::query(
                    r#"
                    INSERT INTO mailbox_item_transfer_entries (
                        id, tenant_id, job_id, ordinal, item_kind, canonical_id, source_item_id, status
                    )
                    VALUES ($1, $2, $3, $4, 'message', $5, $6, 'pending')
                    "#,
                )
                .bind(entry.id)
                .bind(principal.tenant_id)
                .bind(job_id)
                .bind(entry.ordinal)
                .bind(entry.canonical_id)
                .bind(&entry.source_item_id)
                .execute(&mut *tx)
                .await?;
                entries.push(entry);
            }
            tx.commit().await?;
            self.append_audit_event(principal.tenant_id, audit).await?;
            Ok(EwsTransferJob {
                id: job_id,
                direction: direction.to_string(),
                status: "requested".to_string(),
                total_items: item_ids.len(),
                entries,
            })
        })
    }

    };
}

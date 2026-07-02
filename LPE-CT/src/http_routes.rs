use super::*;

pub(crate) async fn health(
    State(state): State<AppState>,
) -> Result<Json<HealthResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        service: "lpe-ct".to_string(),
        node_name: snapshot.site.node_name,
        role: snapshot.site.role,
    }))
}

pub(crate) async fn health_live(
    State(state): State<AppState>,
) -> Result<Json<HealthResponse>, ApiError> {
    health(State(state)).await
}

pub(crate) async fn health_ready(
    State(state): State<AppState>,
) -> Result<Json<ReadinessResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    let mut checks = Vec::new();

    checks.push(match integration_shared_secret() {
        Ok(_) => readiness_ok(
            "integration-secret",
            true,
            "shared LPE/LPE-CT integration secret is configured",
        ),
        Err(error) => readiness_failed(
            "integration-secret",
            true,
            format!("integration secret is invalid: {error}"),
        ),
    });

    checks.push(ha_activation_check());

    checks.push(check_dashboard_state_store(&snapshot.local_data_stores));
    checks.push(check_spool_layout(&state.spool_dir));
    checks.push(check_local_data_store_policy(&snapshot.local_data_stores));
    checks.push(check_non_empty_value(
        "core-delivery-base-url",
        true,
        &snapshot.relay.core_delivery_base_url,
        "core delivery base URL is configured",
        "core delivery base URL is missing",
    ));
    checks.push(
        check_optional_http_dependency(
            "core-delivery-api",
            &format!(
                "{}/health/live",
                snapshot.relay.core_delivery_base_url.trim_end_matches('/')
            ),
            &format!(
                "core delivery API reachable at {}",
                snapshot.relay.core_delivery_base_url
            ),
            "core delivery API unreachable; inbound mail will remain queued locally until recovery",
        )
        .await,
    );
    checks.push(
        check_optional_tcp_dependency(
            "smart-host-reachability",
            &snapshot.relay.primary_upstream,
            "configured upstream smart host accepted a TCP connection",
            "configured upstream smart host is unreachable; direct MX delivery remains available when no smart host route is selected",
        )
        .await,
    );
    checks.push(check_spool_pressure(&state.spool_dir));
    checks.push(check_quarantine_backlog(&state.spool_dir));

    Ok(Json(ReadinessResponse {
        status: readiness_status(&checks).to_string(),
        service: "lpe-ct".to_string(),
        node_name: snapshot.site.node_name,
        role: snapshot.site.role,
        warnings: checks.iter().filter(|check| check.status == "warn").count() as u32,
        checks,
    }))
}

pub(crate) async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    let email = payload.email.trim().to_lowercase();
    if email != snapshot.management_auth.admin_email.trim().to_lowercase()
        || !verify_password(&snapshot.management_auth.password_hash, &payload.password)
    {
        observability::record_security_event("management_auth_failure");
        append_audit_event_with_actor(
            &state,
            &email,
            "management-login-failed",
            "Invalid LPE-CT management credentials",
        )
        .await?;
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid management credentials",
        ));
    }

    let token = Uuid::new_v4().to_string();
    state
        .sessions
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned"))?
        .insert(
            token.clone(),
            ManagementSession {
                email: email.clone(),
                auth_method: "password".to_string(),
            },
        );
    append_audit_event_with_actor(
        &state,
        &email,
        "management-login-succeeded",
        "LPE-CT management session opened",
    )
    .await?;

    Ok(Json(LoginResponse {
        token,
        admin: ManagementIdentity {
            email,
            auth_method: "password".to_string(),
        },
    }))
}

pub(crate) async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<HealthResponse>, ApiError> {
    if let Some(token) = bearer_token(&headers) {
        let session = {
            state
                .sessions
                .lock()
                .map_err(|_| {
                    ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned")
                })?
                .remove(&token)
        };
        if let Some(session) = session {
            append_audit_event_with_actor(
                &state,
                &session.email,
                "management-logout",
                "LPE-CT management session closed",
            )
            .await?;
        }
    }

    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        service: "lpe-ct".to_string(),
        node_name: "management".to_string(),
        role: "management".to_string(),
    }))
}

pub(crate) async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ManagementIdentity>, ApiError> {
    let session = require_management_admin(&state, &headers)?;
    Ok(Json(ManagementIdentity {
        email: session.email,
        auth_method: session.auth_method,
    }))
}

pub(crate) async fn dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DashboardResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let mut snapshot = read_state(&state)?;
    snapshot.queues = smtp::queue_metrics(&state.spool_dir, snapshot.queues.upstream_reachable)
        .map_err(ApiError::from)?;
    Ok(Json(DashboardResponse {
        system: system_metrics::collect(&state.spool_dir),
        state: snapshot,
    }))
}

pub(crate) async fn quarantine_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<smtp::QuarantineQuery>,
) -> Result<Json<Vec<smtp::QuarantineSummary>>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let items = smtp::list_quarantine_items(&state.spool_dir, &runtime, query)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(items))
}

pub(crate) async fn mail_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Query<reporting::HistoryQuery>,
) -> Result<Json<reporting::MailHistoryResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let history = reporting::search_mail_history(
        &state.spool_dir,
        &runtime,
        query,
        snapshot.reporting.history_retention_days,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(history))
}

pub(crate) async fn trace_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<reporting::TraceHistoryDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let details = reporting::load_trace_history(
        &state.spool_dir,
        &runtime,
        &trace_id,
        snapshot.reporting.history_retention_days,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(details))
}

pub(crate) async fn trace_details(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let details = smtp::load_trace_details(&state.spool_dir, &trace_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    Ok(Json(details))
}

pub(crate) async fn retry_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::retry_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-retry",
        &format!("requested retry for {}", result.trace_id),
    )
    .await?;
    Ok(Json(result))
}

pub(crate) async fn release_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::release_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-release",
        &format!("requested release for {}", result.trace_id),
    )
    .await?;
    Ok(Json(result))
}

pub(crate) async fn delete_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::delete_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-delete",
        &format!(
            "deleted trace {} from {}",
            result.trace_id, result.from_queue
        ),
    )
    .await?;
    Ok(Json(result))
}

pub(crate) async fn host_logs_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(category): AxumPath<String>,
) -> Result<Json<host_logs::HostLogList>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    host_logs::list(&category)
        .map(Json)
        .map_err(host_log_api_error)
}

pub(crate) async fn host_log_content(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((category, log_id)): AxumPath<(String, String)>,
) -> Result<Json<host_logs::HostLogContent>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    host_logs::read_content(&category, &log_id)
        .map(Json)
        .map_err(host_log_api_error)
}

pub(crate) async fn download_host_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((category, log_id)): AxumPath<(String, String)>,
) -> Result<Response, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let download = host_logs::download(&category, &log_id).map_err(host_log_api_error)?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let filename = download.name.replace(['"', '\\', '/', '\r', '\n'], "_");
    let disposition = format!("attachment; filename=\"{filename}\"");
    response_headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition).map_err(|error| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid download header: {error}"),
            )
        })?,
    );
    Ok((response_headers, download.bytes).into_response())
}

pub(crate) async fn delete_host_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((category, log_id)): AxumPath<(String, String)>,
) -> Result<Json<HealthResponse>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let name = host_logs::delete(&category, &log_id).map_err(host_log_api_error)?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "host-log-delete",
        &format!("deleted host log {category}/{name}"),
    )
    .await?;
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        service: "lpe-ct".to_string(),
        node_name: "management".to_string(),
        role: "management".to_string(),
    }))
}

fn host_log_api_error(error: host_logs::HostLogError) -> ApiError {
    ApiError::new(error.status(), error.message().to_string())
}

pub(crate) async fn route_diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RouteDiagnosticsResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    Ok(Json(RouteDiagnosticsResponse {
        primary_upstream: snapshot.relay.primary_upstream,
        secondary_upstream: snapshot.relay.secondary_upstream,
        routing: snapshot.routing,
        throttling: snapshot.throttling,
    }))
}

pub(crate) async fn policy_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PolicyStatusResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let recipient_verification_operational_state =
        if !snapshot.policies.recipient_verification.enabled {
            "disabled".to_string()
        } else if runtime.core_delivery_base_url.trim().is_empty() {
            "misconfigured".to_string()
        } else if snapshot.local_data_stores.dedicated_postgres.enabled
            && runtime.local_db.database_url.is_none()
        {
            "degraded".to_string()
        } else if integration_shared_secret().is_err() {
            "bridge-misconfigured".to_string()
        } else {
            "active".to_string()
        };
    let dkim_domains = snapshot
        .policies
        .dkim
        .domains
        .iter()
        .map(|domain| DkimDomainStatusView {
            domain: domain.domain.clone(),
            selector: domain.selector.clone(),
            private_key_path: domain.private_key_path.clone(),
            enabled: domain.enabled,
            key_status: dkim_key_status(&domain.private_key_path),
        })
        .collect::<Vec<_>>();
    let active_dkim_domains = dkim_domains
        .iter()
        .filter(|domain| domain.enabled && domain.key_status == "present")
        .count();
    Ok(Json(PolicyStatusResponse {
        recipient_verification: RecipientVerificationStatusView {
            enabled: snapshot.policies.recipient_verification.enabled,
            fail_closed: snapshot.policies.recipient_verification.fail_closed,
            cache_ttl_seconds: snapshot.policies.recipient_verification.cache_ttl_seconds,
            operational_state: recipient_verification_operational_state,
            cache_backend: if snapshot.local_data_stores.dedicated_postgres.enabled
                && runtime.local_db.database_url.is_some()
            {
                "private-postgres".to_string()
            } else if snapshot.local_data_stores.dedicated_postgres.enabled {
                "misconfigured-private-postgres".to_string()
            } else {
                "memory-only".to_string()
            },
        },
        dkim: DkimStatusView {
            enabled: snapshot.policies.dkim.enabled,
            operational_state: if !snapshot.policies.dkim.enabled {
                "disabled".to_string()
            } else if active_dkim_domains == 0 {
                "misconfigured".to_string()
            } else {
                "active".to_string()
            },
            headers: snapshot.policies.dkim.headers.clone(),
            over_sign: snapshot.policies.dkim.over_sign,
            expiration_seconds: snapshot.policies.dkim.expiration_seconds,
            domains: dkim_domains,
        },
    }))
}

pub(crate) async fn accepted_domains(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AcceptedDomain>>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    Ok(Json(snapshot.accepted_domains))
}

pub(crate) async fn create_accepted_domain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AcceptedDomainInput>,
) -> Result<Json<AcceptedDomain>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let domain = accepted_domain_from_input(payload, None)?;
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        if guard
            .accepted_domains
            .iter()
            .any(|item| item.domain == domain.domain)
        {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "accepted domain already exists",
            ));
        }
        guard.accepted_domains.push(domain.clone());
        normalize_accepted_domains(&mut guard.accepted_domains);
        append_dashboard_audit_event(&mut guard, &admin.email, "create-accepted-domain");
        persist_state(&state.state_file, &guard)?;
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    Ok(Json(domain))
}

pub(crate) async fn update_accepted_domain(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(domain_id): AxumPath<String>,
    Json(payload): Json<AcceptedDomainInput>,
) -> Result<Json<AcceptedDomain>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let domain = accepted_domain_from_input(payload, Some(domain_id.clone()))?;
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        if guard
            .accepted_domains
            .iter()
            .any(|item| item.id != domain_id && item.domain == domain.domain)
        {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "accepted domain already exists",
            ));
        }
        let Some(existing) = guard
            .accepted_domains
            .iter_mut()
            .find(|item| item.id == domain_id)
        else {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "accepted domain not found",
            ));
        };
        *existing = domain.clone();
        normalize_accepted_domains(&mut guard.accepted_domains);
        append_dashboard_audit_event(&mut guard, &admin.email, "update-accepted-domain");
        persist_state(&state.state_file, &guard)?;
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    Ok(Json(domain))
}

pub(crate) async fn delete_accepted_domain(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(domain_id): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        let before = guard.accepted_domains.len();
        guard.accepted_domains.retain(|item| item.id != domain_id);
        if guard.accepted_domains.len() == before {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "accepted domain not found",
            ));
        }
        append_dashboard_audit_event(&mut guard, &admin.email, "delete-accepted-domain");
        persist_state(&state.state_file, &guard)?;
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn import_accepted_domains(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ImportAcceptedDomainsRequest>,
) -> Result<Json<Vec<AcceptedDomain>>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let mut imported = payload
        .domains
        .into_iter()
        .map(|input| accepted_domain_from_input(input, None))
        .collect::<Result<Vec<_>, _>>()?;
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        guard.accepted_domains.append(&mut imported);
        normalize_accepted_domains(&mut guard.accepted_domains);
        append_dashboard_audit_event(&mut guard, &admin.email, "import-accepted-domains");
        persist_state(&state.state_file, &guard)?;
        imported = guard.accepted_domains.clone();
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    Ok(Json(imported))
}

pub(crate) async fn test_accepted_domain(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(domain_id): AxumPath<String>,
) -> Result<Json<AcceptedDomainTestResponse>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let core_delivery_base_url = snapshot.relay.core_delivery_base_url.clone();
    let domain = snapshot
        .accepted_domains
        .into_iter()
        .find(|item| item.id == domain_id)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "accepted domain not found"))?;
    let probe = probe_lpe_core_delivery(&core_delivery_base_url).await?;
    let bridge_probe = probe_lpe_recipient_bridge(&core_delivery_base_url, &domain.domain).await?;
    let verified = probe.verified && bridge_probe.reachable;
    if verified && !domain.verified {
        let previous = read_state(&state)?;
        {
            let mut guard = state.store.lock().map_err(|_| {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned")
            })?;
            if mark_accepted_domain_verified(&mut guard.accepted_domains, &domain_id) {
                append_dashboard_audit_event(&mut guard, &admin.email, "verify-accepted-domain");
                persist_state(&state.state_file, &guard)?;
            }
        }
        if let Err(error) = sync_technical_store(&state).await {
            restore_dashboard_state(&state, &previous)?;
            return Err(error);
        }
    }
    Ok(Json(AcceptedDomainTestResponse {
        domain: domain.domain,
        destination_server: domain.destination_server,
        verified,
        checked_url: probe.checked_url,
        checked_bridge_url: bridge_probe.checked_url,
        bridge_reachable: bridge_probe.reachable,
        recipient_verified: bridge_probe.recipient_verified,
        detail: if verified {
            format!(
                "core LPE delivery API is reachable and the signed LPE-CT recipient-verification bridge responded ({})",
                bridge_probe.detail
            )
        } else if !probe.verified {
            probe.detail
        } else {
            bridge_probe.detail
        },
    }))
}

pub(crate) fn mark_accepted_domain_verified(
    domains: &mut [AcceptedDomain],
    domain_id: &str,
) -> bool {
    let Some(domain) = domains.iter_mut().find(|item| item.id == domain_id) else {
        return false;
    };
    if domain.verified {
        return false;
    }
    domain.verified = true;
    true
}

pub(crate) async fn update_site(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SiteProfile>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-site", move |dashboard| {
        dashboard.site = payload;
    })
    .await
}

pub(crate) async fn update_relay(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<RelaySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    validate_relay_settings(&mut payload)?;
    mutate_state(&state, &admin.email, "update-relay", move |dashboard| {
        dashboard.relay = payload;
    })
    .await
}

pub(crate) async fn update_network(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<NetworkSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let current_public_tls = read_state(&state)?.network.public_tls;
    if payload.public_tls.profiles.is_empty() && payload.public_tls.active_profile_id.is_none() {
        payload.public_tls = current_public_tls;
    }
    normalize_public_tls_settings(&mut payload.public_tls);
    mutate_state(&state, &admin.email, "update-network", move |dashboard| {
        dashboard.network = payload;
    })
    .await
}

pub(crate) async fn update_system_ntp(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<system_actions::NtpUpdateRequest>,
) -> Result<Json<system_actions::SystemActionResponse>, ApiError> {
    require_management_admin(&state, &headers)?;
    system_actions::update_ntp(payload)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

pub(crate) async fn sync_system_ntp(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_actions::SystemActionResponse>, ApiError> {
    require_management_admin(&state, &headers)?;
    system_actions::sync_ntp()
        .await
        .map(Json)
        .map_err(ApiError::from)
}

pub(crate) async fn run_apt_update_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_actions::SystemActionResponse>, ApiError> {
    require_management_admin(&state, &headers)?;
    system_actions::apt_update_upgrade()
        .await
        .map(Json)
        .map_err(ApiError::from)
}

pub(crate) async fn run_system_power_action(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(action): AxumPath<String>,
) -> Result<Json<system_actions::SystemActionResponse>, ApiError> {
    require_management_admin(&state, &headers)?;
    system_actions::power_action(&action)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

pub(crate) async fn upload_public_tls_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<PublicTlsUploadRequest>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let (profile, activate) = store_public_tls_profile(&state, payload).map_err(ApiError::from)?;
    mutate_state(
        &state,
        &admin.email,
        "upload-public-tls-profile",
        move |dashboard| {
            if activate {
                dashboard.network.public_tls.active_profile_id = Some(profile.id.clone());
            }
            dashboard.network.public_tls.profiles.push(profile);
            normalize_public_tls_settings(&mut dashboard.network.public_tls);
        },
    )
    .await
}

pub(crate) async fn select_public_tls_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<PublicTlsSelectionRequest>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let selected_profile = {
        let snapshot = read_state(&state)?;
        match payload
            .profile_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(profile_id) => Some(
                snapshot
                    .network
                    .public_tls
                    .profiles
                    .iter()
                    .find(|profile| profile.id == profile_id)
                    .cloned()
                    .ok_or_else(|| {
                        ApiError::new(StatusCode::NOT_FOUND, "public TLS profile not found")
                    })?,
            ),
            None => None,
        }
    };
    if let Some(profile) = &selected_profile {
        validate_tls_pair_from_paths(&profile.cert_path, &profile.key_path)
            .map_err(ApiError::from)?;
    }
    mutate_state(
        &state,
        &admin.email,
        "select-public-tls-profile",
        move |dashboard| {
            dashboard.network.public_tls.active_profile_id =
                selected_profile.as_ref().map(|profile| profile.id.clone());
            normalize_public_tls_settings(&mut dashboard.network.public_tls);
        },
    )
    .await
}

pub(crate) async fn delete_public_tls_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(profile_id): AxumPath<String>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let existing = {
        let snapshot = read_state(&state)?;
        snapshot
            .network
            .public_tls
            .profiles
            .iter()
            .find(|profile| profile.id == profile_id)
            .cloned()
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "public TLS profile not found"))?
    };
    let existing_id = existing.id.clone();
    let existing_id_for_update = existing_id.clone();
    let existing_cert_path = existing.cert_path.clone();
    let existing_key_path = existing.key_path.clone();
    let result = mutate_state(
        &state,
        &admin.email,
        "delete-public-tls-profile",
        move |dashboard| {
            dashboard
                .network
                .public_tls
                .profiles
                .retain(|profile| profile.id != profile_id);
            if dashboard.network.public_tls.active_profile_id.as_deref()
                == Some(&existing_id_for_update)
            {
                dashboard.network.public_tls.active_profile_id = None;
            }
            normalize_public_tls_settings(&mut dashboard.network.public_tls);
        },
    )
    .await;
    if result.is_ok() && existing_id != ENV_PUBLIC_TLS_PROFILE_ID {
        let _ = fs::remove_file(&existing_cert_path);
        let _ = fs::remove_file(&existing_key_path);
    }
    result
}

pub(crate) async fn update_policies(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<PolicySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    normalize_policy_settings(&mut payload);
    let previous = read_state(&state)?;
    mutate_state(&state, &admin.email, "update-policies", move |dashboard| {
        dashboard.policies = payload;
    })
    .await
    .inspect_err(|_| {
        let _ = restore_dashboard_state(&state, &previous);
    })
}

pub(crate) async fn update_updates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<UpdateSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-updates", move |dashboard| {
        dashboard.updates = payload;
    })
    .await
}

pub(crate) async fn reporting_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<reporting::ReportingSnapshot>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let reporting =
        reporting::snapshot(&state.spool_dir, &snapshot.reporting).map_err(ApiError::from)?;
    Ok(Json(reporting))
}

pub(crate) async fn update_reporting(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<reporting::ReportingSettings>,
) -> Result<Json<reporting::ReportingSnapshot>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    reporting::normalize_reporting_settings(&mut payload);
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        guard.reporting = payload;
        append_dashboard_audit_event(&mut guard, &admin.email, "update-reporting");
        persist_state(&state.state_file, &guard)?;
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    let snapshot = read_state(&state)?;
    let reporting =
        reporting::snapshot(&state.spool_dir, &snapshot.reporting).map_err(ApiError::from)?;
    Ok(Json(reporting))
}

pub(crate) async fn run_digest_reports(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<reporting::DigestRunResponse>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let generated_at = current_timestamp();
    let (generated_reports, snapshot) = {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        let generated_reports =
            reporting::run_digest_generation(&state.spool_dir, &mut guard.reporting)
                .map_err(ApiError::from)?;
        guard.audit.insert(
            0,
            AuditEvent {
                timestamp: generated_at.clone(),
                actor: admin.email.clone(),
                action: "run-quarantine-digests".to_string(),
                details: format!(
                    "generated {} quarantine digest report(s)",
                    generated_reports.len()
                ),
            },
        );
        guard.audit.truncate(12);
        persist_state(&state.state_file, &guard)?;
        (generated_reports, guard.clone())
    };
    sync_dashboard_to_postgres(&snapshot)
        .await
        .map_err(ApiError::from)?;
    let next_digest_run_at = read_state(&state)?.reporting.next_digest_run_at;
    Ok(Json(reporting::DigestRunResponse {
        generated_at,
        generated_reports,
        next_digest_run_at,
    }))
}

pub(crate) async fn digest_reports(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<reporting::DigestReportSummary>>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let reports =
        reporting::list_recent_digest_reports(&state.spool_dir, 20).map_err(ApiError::from)?;
    Ok(Json(reports))
}

pub(crate) async fn digest_report_details(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(report_id): AxumPath<String>,
) -> Result<Json<reporting::DigestReportDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let report = reporting::load_digest_report(&state.spool_dir, &report_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "digest report not found"))?;
    Ok(Json(report))
}

pub(crate) async fn system_diagnostic_services(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_diagnostics::ServiceStatusList>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    Ok(Json(system_diagnostics::service_statuses().await))
}

pub(crate) async fn system_diagnostic_service_action(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((service_id, action)): AxumPath<(String, String)>,
) -> Result<Json<system_diagnostics::ServiceStatus>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let status = system_diagnostics::service_action(&service_id, &action).await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "system-service-action",
        &format!("{} {}", action, service_id),
    )
    .await?;
    Ok(Json(status))
}

pub(crate) async fn system_diagnostic_report(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(kind): AxumPath<String>,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    if kind == "mail-queue" {
        let mut snapshot = read_state(&state)?;
        snapshot.queues = smtp::queue_metrics(&state.spool_dir, snapshot.queues.upstream_reachable)
            .map_err(ApiError::from)?;
        return Ok(Json(system_diagnostics::DiagnosticReport {
            title: "Mail Queue".to_string(),
            status: "ok".to_string(),
            detail: "Live LPE-CT spool queue metrics.".to_string(),
            output: serde_json::to_string_pretty(&snapshot.queues)
                .map_err(anyhow::Error::from)
                .map_err(ApiError::from)?,
        }));
    }
    Ok(Json(system_diagnostics::command_diagnostic(&kind).await?))
}

pub(crate) async fn system_health_check(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let Json(readiness) = health_ready(State(state.clone())).await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "system-health-check",
        "Ran LPE-CT readiness diagnostics from Reporting/System Information",
    )
    .await?;
    Ok(Json(system_diagnostics::DiagnosticReport {
        title: "System Health Check".to_string(),
        status: readiness.status.clone(),
        detail: format!(
            "{} checks completed with {} warning(s)",
            readiness.checks.len(),
            readiness.warnings
        ),
        output: serde_json::to_string_pretty(&readiness)
            .map_err(anyhow::Error::from)
            .map_err(ApiError::from)?,
    }))
}

pub(crate) async fn run_system_tool(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<system_diagnostics::ToolRunRequest>,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let tool = payload.tool.clone();
    let report = system_diagnostics::run_tool(payload).await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "system-diagnostic-tool",
        &format!("Ran {tool}"),
    )
    .await?;
    Ok(Json(report))
}

pub(crate) async fn run_spam_test(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<system_diagnostics::SpamTestRequest>,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let report = system_diagnostics::spam_test(payload).await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "spam-test",
        "Ran configured spam-test command against uploaded file",
    )
    .await?;
    Ok(Json(report))
}

pub(crate) async fn connect_lpe_support(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let report = system_diagnostics::support_connect().await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "support-connect",
        "Started configured secure support connection command",
    )
    .await?;
    Ok(Json(report))
}

pub(crate) async fn flush_mail_queue(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let report = system_diagnostics::flush_mail_queue().await?;
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "flush-mail-queue",
        "Ran LPE-CT mail queue flush action",
    )
    .await?;
    Ok(Json(report))
}

pub(crate) async fn outbound_handoff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<OutboundMessageHandoffRequest>,
) -> Result<Json<OutboundMessageHandoffResponse>, ApiError> {
    let request_trace_id = observability::trace_id_from_headers(&headers);
    let queue_id = payload.queue_id;
    let message_id = payload.message_id;
    let internet_message_id = payload.internet_message_id.clone();
    require_integration_request(&headers, OUTBOUND_HANDOFF_PATH, &payload)?;
    if let Some(role) = ha_non_active_role_for_traffic().map_err(ApiError::from)? {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept outbound handoff traffic"),
        ));
    }
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let response = smtp::process_outbound_handoff(&state.spool_dir, &runtime, payload)
        .await
        .map_err(ApiError::from)?;
    observability::record_outbound_handoff(response.status.as_str());
    info!(
        trace_id = %response.trace_id,
        upstream_trace_id = %request_trace_id,
        queue_id = %queue_id,
        message_id = %message_id,
        status = response.status.as_str(),
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        "outbound handoff processed"
    );
    Ok(Json(response))
}

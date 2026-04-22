use axum::{extract::State, Json};
use lpe_storage::{HealthResponse, Storage};
use std::time::Duration;

use crate::{
    build_readiness_response, check_optional_http_dependency, ha_activation_check,
    http::internal_error, integration_shared_secret, lpe_ct_base_url, readiness_failed,
    readiness_ok, types::{ApiResult, ReadinessResponse},
};

pub(crate) async fn health(State(storage): State<Storage>) -> ApiResult<HealthResponse> {
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    Ok(Json(dashboard.health))
}

pub(crate) async fn health_live() -> ApiResult<HealthResponse> {
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn health_ready(State(storage): State<Storage>) -> ApiResult<ReadinessResponse> {
    let mut checks = Vec::new();

    checks.push(
        match tokio::time::timeout(
            Duration::from_millis(1_500),
            storage.fetch_admin_dashboard(),
        )
        .await
        {
            Ok(Ok(_)) => readiness_ok("postgresql", true, "primary metadata store reachable"),
            Ok(Err(error)) => readiness_failed(
                "postgresql",
                true,
                format!("database-backed dashboard query failed: {error}"),
            ),
            Err(_) => readiness_failed(
                "postgresql",
                true,
                "database-backed dashboard query timed out",
            ),
        },
    );

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

    checks.push(
        check_optional_http_dependency(
            "lpe-ct-api",
            &format!("{}/health/live", lpe_ct_base_url()),
            "outbound relay API reachable",
            "outbound relay API unreachable; outbound queue will accumulate until recovery",
        )
        .await,
    );

    Ok(Json(build_readiness_response("lpe-admin-api", checks)))
}

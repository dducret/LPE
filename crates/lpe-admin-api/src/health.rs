use axum::{extract::State, Json};
use lpe_storage::{HealthResponse, Storage, StorageMetadataDiagnostics};
use std::time::Duration;

use crate::{
    build_readiness_response, check_optional_http_dependency, ha_activation_check,
    http::internal_error,
    integration_shared_secret, lpe_ct_base_url, readiness_failed, readiness_ok, readiness_warn,
    types::{ApiResult, ReadinessCheck, ReadinessResponse},
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

    checks.push(
        match tokio::time::timeout(
            Duration::from_millis(1_500),
            storage.fetch_storage_metadata_diagnostics(),
        )
        .await
        {
            Ok(result) => storage_metadata_readiness_check(result),
            Err(_) => {
                readiness_failed("storage-metadata", true, "storage metadata query timed out")
            }
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

fn storage_metadata_readiness_check(
    result: anyhow::Result<StorageMetadataDiagnostics>,
) -> ReadinessCheck {
    match result {
        Ok(diagnostics) if diagnostics.critical => {
            readiness_failed("storage-metadata", true, diagnostics.detail)
        }
        Ok(diagnostics) if diagnostics.status == "degraded" => {
            readiness_warn("storage-metadata", diagnostics.detail)
        }
        Ok(diagnostics) => readiness_ok("storage-metadata", true, diagnostics.detail),
        Err(error) => readiness_failed(
            "storage-metadata",
            true,
            format!("storage metadata query failed: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::storage_metadata_readiness_check;
    use lpe_storage::StorageMetadataDiagnostics;

    fn diagnostics(status: &str, critical: bool) -> StorageMetadataDiagnostics {
        StorageMetadataDiagnostics {
            status: status.to_string(),
            critical,
            active_pools: 1,
            invalid_policy_references: 0,
            active_placements_on_inactive_pools: 0,
            missing_active_placements: 0,
            detail: "storage metadata detail".to_string(),
        }
    }

    #[test]
    fn storage_metadata_readiness_fails_critical_degradation() {
        let check = storage_metadata_readiness_check(Ok(diagnostics("degraded", true)));
        assert_eq!(check.name, "storage-metadata");
        assert_eq!(check.status, "failed");
        assert!(check.critical);
    }

    #[test]
    fn storage_metadata_readiness_warns_noncritical_degradation() {
        let check = storage_metadata_readiness_check(Ok(diagnostics("degraded", false)));
        assert_eq!(check.status, "warn");
        assert!(!check.critical);
    }

    #[test]
    fn storage_metadata_readiness_passes_consistent_metadata() {
        let check = storage_metadata_readiness_check(Ok(diagnostics("ok", false)));
        assert_eq!(check.status, "ok");
        assert!(check.critical);
    }
}

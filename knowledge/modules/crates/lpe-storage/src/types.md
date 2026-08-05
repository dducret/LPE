---
type: Rust Module
title: types
resource: crates/lpe-storage/src/types.rs#L1-L629
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-pst-psttransferjobrecord
  - external/serde-serialize
  - external/serde-json-value
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [AdminDashboard](../../../../classes/crates/lpe-storage/src/types/AdminDashboard.md)
- [HealthResponse](../../../../classes/crates/lpe-storage/src/types/HealthResponse.md)
- [OverviewStats](../../../../classes/crates/lpe-storage/src/types/OverviewStats.md)
- [ProtocolStatus](../../../../classes/crates/lpe-storage/src/types/ProtocolStatus.md)
- [MailboxRecord](../../../../classes/crates/lpe-storage/src/types/MailboxRecord.md)
- [AccountRecord](../../../../classes/crates/lpe-storage/src/types/AccountRecord.md)
- [DomainRecord](../../../../classes/crates/lpe-storage/src/types/DomainRecord.md)
- [AliasRecord](../../../../classes/crates/lpe-storage/src/types/AliasRecord.md)
- [ServerAdministrator](../../../../classes/crates/lpe-storage/src/types/ServerAdministrator.md)
- [ServerSettings](../../../../classes/crates/lpe-storage/src/types/ServerSettings.md)
- [SecuritySettings](../../../../classes/crates/lpe-storage/src/types/SecuritySettings.md)
- [LocalAiSettings](../../../../classes/crates/lpe-storage/src/types/LocalAiSettings.md)
- [AntispamSettings](../../../../classes/crates/lpe-storage/src/types/AntispamSettings.md)
- [FilterRule](../../../../classes/crates/lpe-storage/src/types/FilterRule.md)
- [QuarantineItem](../../../../classes/crates/lpe-storage/src/types/QuarantineItem.md)
- [StorageOverview](../../../../classes/crates/lpe-storage/src/types/StorageOverview.md)
- [StoragePoolSummary](../../../../classes/crates/lpe-storage/src/types/StoragePoolSummary.md)
- [StoragePoolConfigSummary](../../../../classes/crates/lpe-storage/src/types/StoragePoolConfigSummary.md)
- [StoragePoolReference](../../../../classes/crates/lpe-storage/src/types/StoragePoolReference.md)
- [StoragePolicyScope](../../../../classes/crates/lpe-storage/src/types/StoragePolicyScope.md)
- [StoragePolicySummary](../../../../classes/crates/lpe-storage/src/types/StoragePolicySummary.md)
- [StoragePolicyOverview](../../../../classes/crates/lpe-storage/src/types/StoragePolicyOverview.md)
- [NewStoragePool](../../../../classes/crates/lpe-storage/src/types/NewStoragePool.md)
- [UpdateStoragePool](../../../../classes/crates/lpe-storage/src/types/UpdateStoragePool.md)
- [StoragePolicyUpdate](../../../../classes/crates/lpe-storage/src/types/StoragePolicyUpdate.md)
- [StorageMetadataDiagnostics](../../../../classes/crates/lpe-storage/src/types/StorageMetadataDiagnostics.md)
- [StorageHealthResponse](../../../../classes/crates/lpe-storage/src/types/StorageHealthResponse.md)
- [StoragePoolHealth](../../../../classes/crates/lpe-storage/src/types/StoragePoolHealth.md)
- [StoragePlacementCounts](../../../../classes/crates/lpe-storage/src/types/StoragePlacementCounts.md)
- [StorageMigrationCounts](../../../../classes/crates/lpe-storage/src/types/StorageMigrationCounts.md)
- [StorageCleanupCounts](../../../../classes/crates/lpe-storage/src/types/StorageCleanupCounts.md)
- [StorageMigrationVisibilityResponse](../../../../classes/crates/lpe-storage/src/types/StorageMigrationVisibilityResponse.md)
- [StorageMigrationJobSummary](../../../../classes/crates/lpe-storage/src/types/StorageMigrationJobSummary.md)
- [StorageCleanupVisibilityResponse](../../../../classes/crates/lpe-storage/src/types/StorageCleanupVisibilityResponse.md)
- [StorageCleanupPlacementSummary](../../../../classes/crates/lpe-storage/src/types/StorageCleanupPlacementSummary.md)
- [AuditEvent](../../../../classes/crates/lpe-storage/src/types/AuditEvent.md)
- [NewAccount](../../../../classes/crates/lpe-storage/src/types/NewAccount.md)
- [UpdateAccount](../../../../classes/crates/lpe-storage/src/types/UpdateAccount.md)
- [NewMailbox](../../../../classes/crates/lpe-storage/src/types/NewMailbox.md)
- [NewDomain](../../../../classes/crates/lpe-storage/src/types/NewDomain.md)
- [UpdateDomain](../../../../classes/crates/lpe-storage/src/types/UpdateDomain.md)
- [NewAlias](../../../../classes/crates/lpe-storage/src/types/NewAlias.md)
- [AuditEntryInput](../../../../classes/crates/lpe-storage/src/types/AuditEntryInput.md)
- [DashboardUpdate](../../../../classes/crates/lpe-storage/src/types/DashboardUpdate.md)
- [NewServerAdministrator](../../../../classes/crates/lpe-storage/src/types/NewServerAdministrator.md)
- [NewFilterRule](../../../../classes/crates/lpe-storage/src/types/NewFilterRule.md)
- [EmailTraceSearchInput](../../../../classes/crates/lpe-storage/src/types/EmailTraceSearchInput.md)
- [EmailTraceResult](../../../../classes/crates/lpe-storage/src/types/EmailTraceResult.md)
- [SieveScriptSummary](../../../../classes/crates/lpe-storage/src/types/SieveScriptSummary.md)
- [SieveScriptDocument](../../../../classes/crates/lpe-storage/src/types/SieveScriptDocument.md)
- [MailboxRule](../../../../classes/crates/lpe-storage/src/types/MailboxRule.md)
- [OutlookProfileState](../../../../classes/crates/lpe-storage/src/types/OutlookProfileState.md)
- [MailFlowEntry](../../../../classes/crates/lpe-storage/src/types/MailFlowEntry.md)
- [OutboundQueueStatusUpdate](../../../../classes/crates/lpe-storage/src/types/OutboundQueueStatusUpdate.md)

# Imports

- `crate::pst::PstTransferJobRecord`
- `serde::Serialize`
- `serde_json::Value`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)
---
type: Rust Module
title: types
resource: crates/lpe-exchange/src/store/types.rs#L1-L777
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-storage-collaborationrights
  - external/uuid-uuid
  - external/crate-mapi-notifications-mapinotificationevent
  - external/crate-mapi-properties-mapinamedproperty
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiLocalReplicaDeletedRange](../../../../../classes/crates/lpe-exchange/src/store/types/MapiLocalReplicaDeletedRange.md)
- [MapiEventCreateOutcome](../../../../../classes/crates/lpe-exchange/src/store/types/MapiEventCreateOutcome.md)
- [MapiContactCreateOutcome](../../../../../classes/crates/lpe-exchange/src/store/types/MapiContactCreateOutcome.md)
- [MapiIdentityObjectKind](../../../../../classes/crates/lpe-exchange/src/store/types/MapiIdentityObjectKind.md)
- [as_str](../../../../../functions/crates/lpe-exchange/src/store/types/MapiIdentityObjectKind/as_str.md)
- [MapiNavigationShortcutClientProperties](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNavigationShortcutClientProperties.md)
- [MapiNavigationShortcutRecord](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNavigationShortcutRecord.md)
- [UpsertMapiNavigationShortcutInput](../../../../../classes/crates/lpe-exchange/src/store/types/UpsertMapiNavigationShortcutInput.md)
- [CommitMapiNavigationShortcutCreateInput](../../../../../classes/crates/lpe-exchange/src/store/types/CommitMapiNavigationShortcutCreateInput.md)
- [MapiNavigationShortcutCommit](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNavigationShortcutCommit.md)
- [MapiFaiImportedIdentity](../../../../../classes/crates/lpe-exchange/src/store/types/MapiFaiImportedIdentity.md)
- [CommitMapiNavigationShortcutImportInput](../../../../../classes/crates/lpe-exchange/src/store/types/CommitMapiNavigationShortcutImportInput.md)
- [MapiFaiImportDisposition](../../../../../classes/crates/lpe-exchange/src/store/types/MapiFaiImportDisposition.md)
- [changes_server_replica](../../../../../functions/crates/lpe-exchange/src/store/types/MapiFaiImportDisposition/changes_server_replica.md)
- [MapiNavigationShortcutImportCommit](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNavigationShortcutImportCommit.md)
- [MapiFaiImportConflict](../../../../../classes/crates/lpe-exchange/src/store/types/MapiFaiImportConflict.md)
- [fmt](../../../../../functions/crates/lpe-exchange/src/store/types/MapiFaiImportConflict/std-fmt-display/fmt.md)
- [MapiFaiImportObjectDeleted](../../../../../classes/crates/lpe-exchange/src/store/types/MapiFaiImportObjectDeleted.md)
- [fmt](../../../../../functions/crates/lpe-exchange/src/store/types/MapiFaiImportObjectDeleted/std-fmt-display/fmt.md)
- [MapiAssociatedConfigRecord](../../../../../classes/crates/lpe-exchange/src/store/types/MapiAssociatedConfigRecord.md)
- [UpsertMapiAssociatedConfigInput](../../../../../classes/crates/lpe-exchange/src/store/types/UpsertMapiAssociatedConfigInput.md)
- [CommitMapiAssociatedConfigImportInput](../../../../../classes/crates/lpe-exchange/src/store/types/CommitMapiAssociatedConfigImportInput.md)
- [MapiAssociatedConfigCommit](../../../../../classes/crates/lpe-exchange/src/store/types/MapiAssociatedConfigCommit.md)
- [MapiAssociatedConfigImportCommit](../../../../../classes/crates/lpe-exchange/src/store/types/MapiAssociatedConfigImportCommit.md)
- [MapiIdentityRequest](../../../../../classes/crates/lpe-exchange/src/store/types/MapiIdentityRequest.md)
- [MapiIdentityRecord](../../../../../classes/crates/lpe-exchange/src/store/types/MapiIdentityRecord.md)
- [MapiIdentityLookupRecord](../../../../../classes/crates/lpe-exchange/src/store/types/MapiIdentityLookupRecord.md)
- [MapiSpecialFolderAlias](../../../../../classes/crates/lpe-exchange/src/store/types/MapiSpecialFolderAlias.md)
- [MapiNamedPropertyMapping](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNamedPropertyMapping.md)
- [MapiCustomPropertyObjectKind](../../../../../classes/crates/lpe-exchange/src/store/types/MapiCustomPropertyObjectKind.md)
- [as_str](../../../../../functions/crates/lpe-exchange/src/store/types/MapiCustomPropertyObjectKind/as_str.md)
- [MapiCustomPropertyValue](../../../../../classes/crates/lpe-exchange/src/store/types/MapiCustomPropertyValue.md)
- [MapiFolderProfilePropertyValue](../../../../../classes/crates/lpe-exchange/src/store/types/MapiFolderProfilePropertyValue.md)
- [MapiCheckpointKind](../../../../../classes/crates/lpe-exchange/src/store/types/MapiCheckpointKind.md)
- [as_str](../../../../../functions/crates/lpe-exchange/src/store/types/MapiCheckpointKind/as_str.md)
- [MapiSyncCheckpoint](../../../../../classes/crates/lpe-exchange/src/store/types/MapiSyncCheckpoint.md)
- [MapiNotificationPoll](../../../../../classes/crates/lpe-exchange/src/store/types/MapiNotificationPoll.md)
- [EwsUserConfiguration](../../../../../classes/crates/lpe-exchange/src/store/types/EwsUserConfiguration.md)
- [EwsUserConfigurationKey](../../../../../classes/crates/lpe-exchange/src/store/types/EwsUserConfigurationKey.md)
- [UpsertEwsUserConfigurationInput](../../../../../classes/crates/lpe-exchange/src/store/types/UpsertEwsUserConfigurationInput.md)
- [EwsRetentionPolicyTag](../../../../../classes/crates/lpe-exchange/src/store/types/EwsRetentionPolicyTag.md)
- [EwsSearchableMailbox](../../../../../classes/crates/lpe-exchange/src/store/types/EwsSearchableMailbox.md)
- [EwsDiscoverySearchConfig](../../../../../classes/crates/lpe-exchange/src/store/types/EwsDiscoverySearchConfig.md)
- [EwsDiscoverySearchItem](../../../../../classes/crates/lpe-exchange/src/store/types/EwsDiscoverySearchItem.md)
- [EwsDiscoverySearchResult](../../../../../classes/crates/lpe-exchange/src/store/types/EwsDiscoverySearchResult.md)
- [EwsMessageTrackingReport](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMessageTrackingReport.md)
- [EwsMessageTrackingEvent](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMessageTrackingEvent.md)
- [EwsMessageTrackingReportDetail](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMessageTrackingReportDetail.md)
- [EwsHoldMailbox](../../../../../classes/crates/lpe-exchange/src/store/types/EwsHoldMailbox.md)
- [EwsNonIndexableReport](../../../../../classes/crates/lpe-exchange/src/store/types/EwsNonIndexableReport.md)
- [EwsTransferEntry](../../../../../classes/crates/lpe-exchange/src/store/types/EwsTransferEntry.md)
- [EwsTransferJob](../../../../../classes/crates/lpe-exchange/src/store/types/EwsTransferJob.md)
- [EwsMailAppManifest](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMailAppManifest.md)
- [EwsMailAppInstall](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMailAppInstall.md)
- [EwsMailAppTokenEvent](../../../../../classes/crates/lpe-exchange/src/store/types/EwsMailAppTokenEvent.md)
- [EwsAppMarketplacePolicy](../../../../../classes/crates/lpe-exchange/src/store/types/EwsAppMarketplacePolicy.md)
- [default](../../../../../functions/crates/lpe-exchange/src/store/types/EwsAppMarketplacePolicy/default/default.md)
- [EwsUnifiedMessagingCall](../../../../../classes/crates/lpe-exchange/src/store/types/EwsUnifiedMessagingCall.md)
- [EwsDelegatePreferences](../../../../../classes/crates/lpe-exchange/src/store/types/EwsDelegatePreferences.md)
- [default](../../../../../functions/crates/lpe-exchange/src/store/types/EwsDelegatePreferences/default/default.md)
- [EwsDelegate](../../../../../classes/crates/lpe-exchange/src/store/types/EwsDelegate.md)
- [UpsertEwsDelegateInput](../../../../../classes/crates/lpe-exchange/src/store/types/UpsertEwsDelegateInput.md)
- [MapiAssociatedConfigChange](../../../../../classes/crates/lpe-exchange/src/store/types/MapiAssociatedConfigChange.md)
- [MapiSyncChangeSet](../../../../../classes/crates/lpe-exchange/src/store/types/MapiSyncChangeSet.md)
- [default](../../../../../functions/crates/lpe-exchange/src/store/types/MapiSyncChangeSet/default/default.md)
- [ExchangeAddressBookEntry](../../../../../classes/crates/lpe-exchange/src/store/types/ExchangeAddressBookEntry.md)
- [ExchangeAddressBookEntryDetails](../../../../../classes/crates/lpe-exchange/src/store/types/ExchangeAddressBookEntryDetails.md)
- [EwsImGroup](../../../../../classes/crates/lpe-exchange/src/store/types/EwsImGroup.md)
- [EwsImGroupMember](../../../../../classes/crates/lpe-exchange/src/store/types/EwsImGroupMember.md)
- [EwsImList](../../../../../classes/crates/lpe-exchange/src/store/types/EwsImList.md)
- [EwsImMemberInput](../../../../../classes/crates/lpe-exchange/src/store/types/EwsImMemberInput.md)
- [MapiMailboxContentCommitTime](../../../../../classes/crates/lpe-exchange/src/store/types/MapiMailboxContentCommitTime.md)
- [MapiContentTableQuery](../../../../../classes/crates/lpe-exchange/src/store/types/MapiContentTableQuery.md)
- [MapiContentTableQueryResult](../../../../../classes/crates/lpe-exchange/src/store/types/MapiContentTableQueryResult.md)
- [MapiContentTableSort](../../../../../classes/crates/lpe-exchange/src/store/types/MapiContentTableSort.md)
- [MapiContentTableSortField](../../../../../classes/crates/lpe-exchange/src/store/types/MapiContentTableSortField.md)
- [ExchangeAddressBookEntryKind](../../../../../classes/crates/lpe-exchange/src/store/types/ExchangeAddressBookEntryKind.md)
- [ExchangeAddressBookDirectoryKind](../../../../../classes/crates/lpe-exchange/src/store/types/ExchangeAddressBookDirectoryKind.md)

# Imports

- `lpe_storage::CollaborationRights`
- `uuid::Uuid`
- `crate::mapi::notifications::MapiNotificationEvent`
- `crate::mapi::properties::MapiNamedProperty`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_sharing_metadata(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let requested_kind = requested_sharing_kind(request);
        let contacts =
            if requested_kind.is_none_or(|kind| kind == CollaborationResourceKind::Contacts) {
                self.store
                    .fetch_accessible_contact_collections(principal.account_id)
                    .await?
                    .into_iter()
                    .filter(|collection| collection.is_owned)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
        let calendars =
            if requested_kind.is_none_or(|kind| kind == CollaborationResourceKind::Calendar) {
                self.store
                    .fetch_accessible_calendar_collections(principal.account_id)
                    .await?
                    .into_iter()
                    .filter(|collection| collection.is_owned)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
        Ok(get_sharing_metadata_response(
            principal, &contacts, &calendars,
        ))
    }

    pub(in crate::service) async fn get_sharing_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let input = parse_sharing_request(request)?;
            let owner = self
                .resolve_same_tenant_account(principal, &input.owner_email)
                .await?;
            let folder = self
                .accessible_shared_collection(principal, owner.id, input.kind)
                .await?
                .ok_or_else(|| anyhow!("shared folder is not accessible to this account"))?;
            Ok(get_sharing_folder_response(&folder))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "GetSharingFolder",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn refresh_sharing_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let folder_id = requested_collection_id(request)
                .ok_or_else(|| anyhow!("RefreshSharingFolder requires a FolderId."))?;
            let mut collections = self
                .store
                .fetch_accessible_contact_collections(principal.account_id)
                .await?;
            collections.extend(
                self.store
                    .fetch_accessible_calendar_collections(principal.account_id)
                    .await?,
            );
            let Some(collection) = collections
                .iter()
                .find(|collection| collection.id == folder_id)
            else {
                bail!("shared folder is not accessible to this account");
            };
            Ok(refresh_sharing_folder_response(collection))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "RefreshSharingFolder",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn accept_sharing_invitation(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let input = parse_sharing_request(request)?;
        let owner = self
            .resolve_same_tenant_account(principal, &input.owner_email)
            .await?;
        let grant = self
            .store
            .upsert_ews_sharing_grant(
                owner.id,
                &principal.email,
                input.kind,
                input.rights,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-accept-sharing-invitation".to_string(),
                    subject: format!("{}:{}", input.kind.as_str(), owner.id),
                },
            )
            .await?;
        Ok(accept_sharing_invitation_response(&grant))
    }

    async fn accessible_shared_collection(
        &self,
        principal: &AccountPrincipal,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Option<CollaborationCollection>> {
        let collections = match kind {
            CollaborationResourceKind::Contacts => {
                self.store
                    .fetch_accessible_contact_collections(principal.account_id)
                    .await?
            }
            CollaborationResourceKind::Calendar => {
                self.store
                    .fetch_accessible_calendar_collections(principal.account_id)
                    .await?
            }
            CollaborationResourceKind::Tasks => Vec::new(),
        };
        Ok(collections.into_iter().find(|collection| {
            collection.owner_account_id == owner_account_id && !collection.is_owned
        }))
    }

    async fn resolve_same_tenant_account(
        &self,
        principal: &AccountPrincipal,
        email: &str,
    ) -> Result<ExchangeAddressBookEntry> {
        self.store
            .fetch_address_book_entries(principal)
            .await?
            .into_iter()
            .find(|entry| {
                entry.entry_kind == ExchangeAddressBookEntryKind::Account
                    && entry.email.eq_ignore_ascii_case(email)
            })
            .ok_or_else(|| anyhow!("sharing owner account not found in the same tenant"))
    }
}

#[derive(Debug, Clone)]
pub(in crate::service) struct SharingRequest {
    pub(in crate::service) owner_email: String,
    pub(in crate::service) kind: CollaborationResourceKind,
    pub(in crate::service) rights: CollaborationRights,
}

pub(in crate::service) fn get_sharing_metadata_response(
    principal: &AccountPrincipal,
    contact_collections: &[CollaborationCollection],
    calendar_collections: &[CollaborationCollection],
) -> String {
    let entries = contact_collections
        .iter()
        .chain(calendar_collections.iter())
        .map(|collection| sharing_metadata_entry_xml(principal, collection))
        .collect::<String>();
    format!(
        concat!(
            "<m:GetSharingMetadataResponse>",
            "<m:ResponseMessages>",
            "<m:GetSharingMetadataResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SharingMetadata>{entries}</m:SharingMetadata>",
            "</m:GetSharingMetadataResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetSharingMetadataResponse>"
        ),
        entries = entries,
    )
}

pub(in crate::service) fn get_sharing_folder_response(
    collection: &CollaborationCollection,
) -> String {
    format!(
        concat!(
            "<m:GetSharingFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetSharingFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SharingFolder>",
            "<t:FolderId Id=\"{folder_id}\" ChangeKey=\"{change_key}\"/>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "<t:FolderClass>{folder_class}</t:FolderClass>",
            "<t:OwnerSmtpAddress>{owner}</t:OwnerSmtpAddress>",
            "<t:PermissionLevel>{permission}</t:PermissionLevel>",
            "</m:SharingFolder>",
            "</m:GetSharingFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetSharingFolderResponse>"
        ),
        folder_id = escape_xml(&collection.id),
        change_key = folder_change_key(&collection.id),
        display_name = escape_xml(&collection.display_name),
        folder_class = ews_sharing_folder_class(&collection.kind),
        owner = escape_xml(&collection.owner_email),
        permission = ews_permission_level(&collection.rights),
    )
}

pub(in crate::service) fn refresh_sharing_folder_response(
    collection: &CollaborationCollection,
) -> String {
    format!(
        concat!(
            "<m:RefreshSharingFolderResponse>",
            "<m:ResponseMessages>",
            "<m:RefreshSharingFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SharingFolderId>",
            "<t:FolderId Id=\"{folder_id}\" ChangeKey=\"{change_key}\"/>",
            "</m:SharingFolderId>",
            "</m:RefreshSharingFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:RefreshSharingFolderResponse>"
        ),
        folder_id = escape_xml(&collection.id),
        change_key = folder_change_key(&collection.id),
    )
}

pub(in crate::service) fn accept_sharing_invitation_response(grant: &CollaborationGrant) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:AcceptSharingInvitation>",
            "<t:ItemId Id=\"sharing:{kind}:{owner_id}:{grantee_id}\" ChangeKey=\"{updated_at}\"/>",
            "<t:SharedFolderId Id=\"shared-{kind}:{owner_id}\"/>",
            "<t:OwnerSmtpAddress>{owner}</t:OwnerSmtpAddress>",
            "<t:DataType>{data_type}</t:DataType>",
            "<t:PermissionLevel>{permission}</t:PermissionLevel>",
            "</t:AcceptSharingInvitation>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        kind = escape_xml(&grant.kind),
        owner_id = grant.owner_account_id,
        grantee_id = grant.grantee_account_id,
        updated_at = escape_xml(&grant.updated_at),
        owner = escape_xml(&grant.owner_email),
        data_type = ews_sharing_data_type(&grant.kind),
        permission = ews_permission_level(&grant.rights),
    )
}

pub(in crate::service) fn requested_sharing_kind(
    request: &str,
) -> Option<CollaborationResourceKind> {
    let value = element_text(request, "DataType")
        .or_else(|| element_text(request, "FolderClass"))
        .or_else(|| element_text(request, "FolderName"))
        .unwrap_or_else(|| request.to_string())
        .to_ascii_lowercase();
    if value.contains("calendar") {
        Some(CollaborationResourceKind::Calendar)
    } else if value.contains("contact") {
        Some(CollaborationResourceKind::Contacts)
    } else {
        None
    }
}

pub(in crate::service) fn parse_sharing_request(request: &str) -> Result<SharingRequest> {
    let owner_email = element_content(request, "SharedFolderOwner")
        .and_then(parse_mailbox)
        .map(|mailbox| mailbox.address)
        .or_else(|| element_text(request, "OwnerSmtpAddress"))
        .or_else(|| element_text(request, "SharingOwnerSmtpAddress"))
        .or_else(|| element_text(request, "SmtpAddress"))
        .or_else(|| {
            element_content(request, "From")
                .and_then(parse_mailbox)
                .map(|mailbox| mailbox.address)
        })
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("sharing request is missing a same-tenant owner mailbox"))?;
    let kind = requested_sharing_kind(request)
        .ok_or_else(|| anyhow!("sharing request supports only calendar and contacts folders"))?;
    let rights = sharing_rights(request);
    Ok(SharingRequest {
        owner_email,
        kind,
        rights,
    })
}

fn sharing_rights(request: &str) -> CollaborationRights {
    let permission = element_text(request, "PermissionLevel")
        .or_else(|| element_text(request, "SharingPermission"))
        .unwrap_or_else(|| "Reviewer".to_string())
        .to_ascii_lowercase();
    let may_write = permission.contains("editor")
        || permission.contains("author")
        || permission.contains("owner")
        || permission.contains("write");
    CollaborationRights {
        may_read: true,
        may_write,
        may_delete: permission.contains("editor") || permission.contains("owner"),
        may_share: permission.contains("owner"),
    }
}

fn sharing_metadata_entry_xml(
    principal: &AccountPrincipal,
    collection: &CollaborationCollection,
) -> String {
    format!(
        concat!(
            "<t:SharingMetadata>",
            "<t:OwnerSmtpAddress>{owner}</t:OwnerSmtpAddress>",
            "<t:FolderId Id=\"{folder_id}\" ChangeKey=\"{change_key}\"/>",
            "<t:FolderClass>{folder_class}</t:FolderClass>",
            "<t:FolderName>{folder_name}</t:FolderName>",
            "<t:DataType>{data_type}</t:DataType>",
            "<t:InitiatorName>{initiator}</t:InitiatorName>",
            "<t:InitiatorSmtpAddress>{owner}</t:InitiatorSmtpAddress>",
            "</t:SharingMetadata>"
        ),
        owner = escape_xml(&principal.email),
        folder_id = escape_xml(&collection.id),
        change_key = folder_change_key(&collection.id),
        folder_class = ews_sharing_folder_class(&collection.kind),
        folder_name = escape_xml(&collection.display_name),
        data_type = ews_sharing_data_type(&collection.kind),
        initiator = escape_xml(&principal.display_name),
    )
}

fn ews_permission_level(rights: &CollaborationRights) -> &'static str {
    if rights.may_share {
        "Owner"
    } else if rights.may_write || rights.may_delete {
        "Editor"
    } else {
        "Reviewer"
    }
}

fn ews_sharing_folder_class(kind: &str) -> &'static str {
    match kind {
        "calendar" => "IPF.Appointment",
        "contacts" => "IPF.Contact",
        _ => "IPF.Note",
    }
}

fn ews_sharing_data_type(kind: &str) -> &'static str {
    match kind {
        "calendar" => "Calendar",
        "contacts" => "Contacts",
        _ => "Unknown",
    }
}

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
        let result = async {
            let input = parse_sharing_metadata_request(request)?;
            let mut collections = Vec::new();
            if input
                .kind
                .is_none_or(|kind| kind == CollaborationResourceKind::Contacts)
            {
                collections.extend(
                    self.store
                        .fetch_accessible_contact_collections(principal.account_id)
                        .await?
                        .into_iter()
                        .filter(|collection| collection.is_owned),
                );
            }
            if input
                .kind
                .is_none_or(|kind| kind == CollaborationResourceKind::Calendar)
            {
                collections.extend(
                    self.store
                        .fetch_accessible_calendar_collections(principal.account_id)
                        .await?
                        .into_iter()
                        .filter(|collection| collection.is_owned),
                );
            }
            let matching = collections
                .into_iter()
                .filter(|collection| collection.id == input.collection_id)
                .collect::<Vec<_>>();
            let [collection] = matching.as_slice() else {
                bail!("GetSharingMetadata requires one collection owned by the authenticated account");
            };
            let contacts = (collection.kind == "contacts")
                .then(|| collection.clone())
                .into_iter()
                .collect::<Vec<_>>();
            let calendars = (collection.kind == "calendar")
                .then(|| collection.clone())
                .into_iter()
                .collect::<Vec<_>>();
            Ok(get_sharing_metadata_response(principal, &contacts, &calendars))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "GetSharingMetadata",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
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
        let invitation = element_content(request, "AcceptSharingInvitation")
            .ok_or_else(|| anyhow!("AcceptSharingInvitation payload is missing"))?;
        let invitation_data = element_content(invitation, "SharingInvitationData")
            .ok_or_else(|| anyhow!("AcceptSharingInvitation payload is missing"))?;
        let input = parse_sharing_request(invitation_data)?;
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
        let category = match input.kind {
            CollaborationResourceKind::Contacts => "contacts",
            CollaborationResourceKind::Calendar => "calendar",
            CollaborationResourceKind::Tasks => "tasks",
        };
        let revision = self
            .store
            .fetch_account_category_modseq(owner.id, category)
            .await?;
        Ok(accept_sharing_invitation_response(
            &grant,
            &versioned_change_key("sharing", &grant.id.to_string(), &revision.to_string()),
        ))
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

struct SharingMetadataRequest {
    collection_id: String,
    kind: Option<CollaborationResourceKind>,
}

// [MS-OXWSCORE] section 3.1.4.2: validate the complete bounded response
// object collection before the canonical grant upsert begins.
pub(in crate::service) fn validate_accept_sharing_invitation_shape(request: &str) -> Result<()> {
    let item_collections = element_contents(request, "Items");
    let [items] = item_collections.as_slice() else {
        bail!("CreateItem requires exactly one Items collection");
    };
    let item_count = ["Message", "Contact", "CalendarItem", "Task", "AcceptSharingInvitation"]
        .into_iter()
        .map(|name| element_contents(items, name).len())
        .sum::<usize>();
    if item_count != 1 || element_contents(items, "AcceptSharingInvitation").len() != 1 {
        bail!("CreateItem supports exactly one AcceptSharingInvitation item");
    }
    let invitation_data = element_contents(items, "SharingInvitationData");
    let [invitation_data] = invitation_data.as_slice() else {
        bail!("AcceptSharingInvitation requires exactly one SharingInvitationData payload");
    };
    let owners = element_contents(invitation_data, "SharedFolderOwner");
    let [owner] = owners.as_slice() else {
        bail!("AcceptSharingInvitation requires exactly one same-tenant sharing owner");
    };
    if parse_mailbox(owner).is_none()
        || element_contents(invitation_data, "DataType").len() != 1
        || element_contents(invitation_data, "PermissionLevel").len() > 1
    {
        bail!("AcceptSharingInvitation has an invalid canonical sharing payload");
    }
    Ok(())
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

pub(in crate::service) fn accept_sharing_invitation_response(
    grant: &CollaborationGrant,
    change_key: &str,
) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:AcceptSharingInvitation>",
            "<t:ItemId Id=\"sharing:{kind}:{owner_id}:{grantee_id}\" ChangeKey=\"{change_key}\"/>",
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
        change_key = escape_xml(change_key),
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

fn parse_sharing_metadata_request(request: &str) -> Result<SharingMetadataRequest> {
    let folders = element_contents(request, "IdOfFolderToShare");
    let [folder] = folders.as_slice() else {
        bail!("GetSharingMetadata requires exactly one IdOfFolderToShare");
    };
    let ids = attribute_values_for_tag(folder, "FolderId", "Id")
        .into_iter()
        .chain(attribute_values_for_tag(folder, "DistinguishedFolderId", "Id"))
        .collect::<Vec<_>>();
    let [id] = ids.as_slice() else {
        bail!("GetSharingMetadata requires exactly one canonical contact or calendar folder id");
    };
    let kind = requested_sharing_kind(folder);
    let collection_id = match *id {
        "contacts" | "calendar" => DEFAULT_COLLECTION_ID.to_string(),
        other => other.to_string(),
    };
    Ok(SharingMetadataRequest {
        collection_id,
        kind,
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

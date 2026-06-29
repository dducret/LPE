use super::super::*;

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

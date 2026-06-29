use super::super::*;

pub(in crate::service) fn convert_id_success_response(alternate_ids: String) -> String {
    format!(
        concat!(
            "<m:ConvertIdResponse>",
            "<m:ResponseMessages>",
            "<m:ConvertIdResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{alternate_ids}",
            "</m:ConvertIdResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ConvertIdResponse>"
        ),
        alternate_ids = alternate_ids,
    )
}

pub(in crate::service) fn convert_id_xml(output: &ConvertIdOutput) -> String {
    let element = match output.family {
        "public-folder" => "AlternatePublicFolderId",
        "public-folder-item" => "AlternatePublicFolderItemId",
        _ => "AlternateId",
    };
    format!(
        "<t:{element} Format=\"{format}\" Id=\"{id}\"/>",
        element = element,
        format = escape_xml(output.format),
        id = escape_xml(&output.id),
    )
}

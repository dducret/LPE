use super::super::*;

pub(in crate::service) fn computed_room_list_address(principal: &AccountPrincipal) -> String {
    let domain = principal
        .email
        .split_once('@')
        .map(|(_, domain)| domain)
        .unwrap_or("local");
    format!("rooms@{domain}")
}

pub(in crate::service) fn get_rooms_response(entries: &[ExchangeAddressBookEntry]) -> String {
    let mut rooms_xml = String::new();
    for entry in entries.iter().filter(|entry| {
        matches!(
            entry.directory_kind,
            ExchangeAddressBookDirectoryKind::Room | ExchangeAddressBookDirectoryKind::Equipment
        )
    }) {
        rooms_xml.push_str(&format!(
            "<t:Room><t:Id><t:Name>{name}</t:Name><t:EmailAddress>{email}</t:EmailAddress></t:Id></t:Room>",
            name = escape_xml(&entry.display_name),
            email = escape_xml(&entry.email),
        ));
    }
    format!(
        concat!(
            "<m:GetRoomsResponse>",
            "<m:ResponseMessages>",
            "<m:GetRoomsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Rooms>{rooms_xml}</m:Rooms>",
            "</m:GetRoomsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetRoomsResponse>"
        ),
        rooms_xml = rooms_xml
    )
}

pub(in crate::service) fn get_room_lists_response(
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let has_rooms = entries.iter().any(|entry| {
        matches!(
            entry.directory_kind,
            ExchangeAddressBookDirectoryKind::Room | ExchangeAddressBookDirectoryKind::Equipment
        )
    });
    let room_lists_xml = if has_rooms {
        let address = computed_room_list_address(principal);
        format!(
            "<t:Address><t:Name>Rooms</t:Name><t:EmailAddress>{address}</t:EmailAddress><t:RoutingType>SMTP</t:RoutingType><t:MailboxType>PublicDL</t:MailboxType></t:Address>",
            address = escape_xml(&address),
        )
    } else {
        String::new()
    };
    format!(
        concat!(
            "<m:GetRoomListsResponse>",
            "<m:ResponseMessages>",
            "<m:GetRoomListsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RoomLists>{room_lists_xml}</m:RoomLists>",
            "</m:GetRoomListsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetRoomListsResponse>"
        ),
        room_lists_xml = room_lists_xml
    )
}

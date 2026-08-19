use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_rooms(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            if let Some(room_list) = requested_room_list_address(request)? {
                let expected = computed_room_list_address(principal);
                if !room_list.eq_ignore_ascii_case(&expected) {
                    bail!(
                        "GetRooms supports only LPE's computed tenant room/resource list; explicit room-list membership is not supported."
                    );
                }
            }
            let entries = self.store.fetch_address_book_entries(principal).await?;
            Ok(get_rooms_response(&entries))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("GetRooms", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    pub(in crate::service) async fn get_room_lists(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let entries = self.store.fetch_address_book_entries(principal).await?;
        Ok(get_room_lists_response(principal, &entries))
    }
}

pub(in crate::service) fn computed_room_list_address(principal: &AccountPrincipal) -> String {
    let domain = principal
        .email
        .split_once('@')
        .map(|(_, domain)| domain)
        .unwrap_or("local");
    format!("rooms@{domain}")
}

/// [MS-OXWSGTRM] §3.1.4.2.3.2 defines one RoomList request element. Validate
/// the bounded LPE selector before reading the canonical directory projection.
pub(in crate::service) fn requested_room_list_address(request: &str) -> Result<Option<String>> {
    let room_lists = element_contents(request, "RoomList");
    let Some(room_list) = room_lists.first() else {
        return Ok(None);
    };
    if room_lists.len() != 1 {
        bail!("GetRooms accepts at most one RoomList");
    }

    let email_addresses = element_contents(room_list, "EmailAddress");
    let addresses = element_contents(room_list, "Address");
    if email_addresses.len() + addresses.len() > 1 {
        bail!("GetRooms RoomList accepts at most one address");
    }

    Ok(email_addresses
        .first()
        .map(|value| xml_text(value))
        .or_else(|| addresses.first().map(|value| xml_text(value)))
        .filter(|value| !value.trim().is_empty()))
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

---
type: Rust Method
title: fetch_address_book_entries
resource: crates/lpe-exchange/src/tests/mod.rs#L7470-L7605
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/fake_contact_phone_by_label
  - functions/crates/lpe-exchange/src/tests/fake_contact_phone_values_by_label
  - functions/crates/lpe-exchange/src/tests/fake_contact_address_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/resolve_names
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/expand_dl
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/find_people
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_persona
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips
  - functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms
  - functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_room_lists
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries
---

# Signature

`fn fetch_address_book_entries<'a>( &'a self, principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fake_contact_phone_by_label](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_contact_phone_by_label.md)
- [fake_contact_phone_values_by_label](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_contact_phone_values_by_label.md)
- [fake_contact_address_value](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_contact_address_value.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_dn_to_mid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)
- [resolve_names](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/resolve_names.md)
- [expand_dl](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/expand_dl.md)
- [find_people](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/find_people.md)
- [get_persona](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_persona.md)
- [get_user_photo](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo.md)
- [get_mail_tips](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips.md)
- [get_rooms](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms.md)
- [get_room_lists](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_room_lists.md)
- [resolve_same_tenant_account](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account.md)
- [add_distribution_group_to_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list.md)
- [rpc_proxy_address_book_entries](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries.md)
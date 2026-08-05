---
type: Rust Function
title: update_antispam_settings
resource: crates/lpe-admin-api/src/console.rs#L640-L656
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn update_antispam_settings( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpdateAntispamSettingsRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
---
type: Rust Function
title: antivirus_provider_from_env
resource: LPE-CT/src/smtp/antivirus.rs#L58-L126
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers
---

# Signature

`fn antivirus_provider_from_env(provider_id: &str) -> Option<AntivirusProviderConfig>`

# Called by

- [load_antivirus_providers](../../../../../functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers.md)
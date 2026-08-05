---
type: Rust Function
title: load_antivirus_providers
resource: LPE-CT/src/smtp/antivirus.rs#L51-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/antivirus_provider_from_env
  called_by:
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/tests/takeri_provider_loads_with_default_command_and_args
---

# Signature

`pub(crate) fn load_antivirus_providers(provider_chain: &[String]) -> Vec<AntivirusProviderConfig>`

# Calls

- [antivirus_provider_from_env](../../../../../functions/LPE-CT/src/smtp/antivirus/antivirus_provider_from_env.md)

# Called by

- [runtime_config_from_dashboard](../../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [takeri_provider_loads_with_default_command_and_args](../../../../../functions/LPE-CT/src/smtp/tests/takeri_provider_loads_with_default_command_and_args.md)
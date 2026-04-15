# LPE-CT

## Francais

`LPE-CT` est le centre de tri `LPE` pour un serveur separe place en `DMZ`. Ce composant isole l'entree SMTP, les politiques de quarantaine, le relais vers le coeur `LPE` en `LAN`, et son interface de management dediee.

### Objectifs

- exposer en DMZ un noeud de tri distinct du serveur coeur
- limiter les flux vers le LAN aux relais et a la gestion explicitement autorises
- fournir une interface de management locale orientee exploitation
- conserver une installation `Debian Trixie` simple, reproductible et mise a jour depuis Git

### Contenu

- `src/` API de management Rust pour le centre de tri
- `web/` interface statique de management servie par `nginx`
- `installation/debian-trixie/` scripts d'installation, mise a jour et verification
- `docs/architecture/center-de-tri.md` architecture et perimetre DMZ

### Demarrage local

```powershell
cargo run --manifest-path LPE-CT/Cargo.toml
```

Par defaut, l'API ecoute sur `127.0.0.1:8380` et persiste son etat operatoire dans `LPE_CT_STATE_FILE`.

## English

`LPE-CT` is the `LPE` sorting center for a separate server placed in a `DMZ`. This component isolates SMTP ingress, quarantine policies, relay flows toward the core `LPE` services in the `LAN`, and its dedicated management interface.

### Goals

- expose a DMZ edge node separate from the core server
- limit LAN flows to explicitly authorized relay and management traffic
- provide a local management interface focused on operations
- keep `Debian Trixie` installation simple, reproducible, and Git-driven

### Contents

- `src/` Rust management API for the sorting center
- `web/` static management interface served by `nginx`
- `installation/debian-trixie/` install, update, and verification scripts
- `docs/architecture/center-de-tri.md` DMZ architecture and scope

### Local start

```powershell
cargo run --manifest-path LPE-CT/Cargo.toml
```

By default, the API listens on `127.0.0.1:8380` and persists its operating state in `LPE_CT_STATE_FILE`.

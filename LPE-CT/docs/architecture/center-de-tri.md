# Centre de tri DMZ | DMZ sorting center

## Francais

### Role

`LPE-CT` est un composant de bord pour une installation `LPE` ou le serveur de tri reside dans une `DMZ` distincte du `LAN`.

Le centre de tri:

- recoit le trafic SMTP expose publiquement
- applique des politiques de filtrage, quarantaine et drainage
- remet les messages entrants acceptes vers le coeur `LPE` cote `LAN` via une API interne explicite
- recoit aussi les handoffs sortants emis par `LPE`, puis execute le relais `SMTP`
- expose une interface de management via `nginx` et une API locale Rust

La v1 fonctionnelle inclut un listener SMTP minimal, un spool local, une quarantaine simple, un mode drainage, un endpoint de handoff interne pour la sortie, et une remise finale HTTP vers `LPE` pour l'entree. Le `mTLS` est conserve comme politique de configuration mais n'est pas active par defaut dans cette v1 tant que le choix TLS conforme aux licences n'est pas documente.

### Positionnement d'architecture

`LPE-CT` ne remplace pas le coeur `LPE`:

- les donnees metier canoniques restent dans le coeur `LPE`
- `PostgreSQL` reste le stockage principal pour le produit coeur
- `LPE-CT` conserve seulement un etat operatoire local minimal de configuration et d'exploitation
- l'axe moderne du produit reste `JMAP` sur le coeur `LPE`

### Flux reseau

Flux autorises depuis Internet vers la `DMZ`:

- SMTP entrant vers `LPE-CT`
- HTTPS ou HTTP d'administration selon la politique d'exposition retenue

Flux autorises de la `DMZ` vers le `LAN`:

- remise finale HTTP authentifiee vers les noeuds coeur designes
- trafic de management strictement borne aux adresses et segments autorises

Flux interdits par defaut:

- acces direct de la `DMZ` aux bases `PostgreSQL` du coeur
- exposition du back office coeur sur le serveur DMZ
- sortie de donnees vers un service IA externe

### Interface de management

L'interface de management `LPE-CT` couvre:

- identite du noeud et bind publics
- relais primaires et secondaires vers le `LAN`
- politique de surface reseau et CIDR autorises
- mode drainage, quarantaine et controles `SPF` / `DKIM` / `DMARC`
- fenetre et source de mise a jour Git-first

### Installation Debian

Les scripts `Debian Trixie` de `LPE-CT`:

- installent les prerequis systeme
- clonent le depot Git dans `/opt/lpe-ct/src`
- compilent le binaire `lpe-ct`
- creent le spool `/var/spool/lpe-ct`
- deploient l'interface statique et la configuration `nginx`
- installent et redemarrent `lpe-ct.service`

Le service systemd donne au binaire la capacite `CAP_NET_BIND_SERVICE` afin d'ecouter sur le port SMTP `25` sans executer le service en root.

### Coherence produit

Cette decomposition permet de garder:

- le coeur `LPE` sur le `LAN` pour les donnees metier et `JMAP`
- le centre de tri en `DMZ` pour les flux exposables
- une trajectoire compatible avec une IA locale future sans sortie de donnees

## English

### Role

`LPE-CT` is an edge component for an `LPE` deployment where the sorting server resides in a `DMZ` separate from the `LAN`.

The sorting center:

- receives publicly exposed SMTP traffic
- applies filtering, quarantine, and drain policies
- delivers accepted inbound messages to the core `LPE` services on the `LAN` through an explicit internal API
- also receives outbound handoffs emitted by `LPE`, then executes `SMTP` relay
- exposes a management interface through `nginx` and a local Rust API

The functional v1 includes a minimal SMTP listener, local spool, simple quarantine, drain mode, an internal handoff endpoint for outbound work, and HTTP final delivery into `LPE` for inbound mail. `mTLS` remains a configuration policy but is not enabled by default in this v1 until the license-compliant TLS choice is documented.

### Architecture position

`LPE-CT` does not replace the core `LPE` stack:

- canonical business data remains in the core `LPE` services
- `PostgreSQL` remains the primary store for the core product
- `LPE-CT` keeps only minimal local operational state for configuration and operations
- the modern product axis remains `JMAP` on the core `LPE` side

### Network flows

Flows allowed from the Internet to the `DMZ`:

- inbound SMTP to `LPE-CT`
- administration HTTPS or HTTP depending on the chosen exposure policy

Flows allowed from the `DMZ` to the `LAN`:

- authenticated HTTP final delivery toward designated core nodes
- management traffic strictly limited to authorized addresses and segments

Flows denied by default:

- direct `DMZ` access to the core `PostgreSQL` databases
- exposing the core back office on the DMZ server
- sending data to an external AI service

### Management interface

The `LPE-CT` management interface covers:

- node identity and public bind addresses
- primary and secondary relays toward the `LAN`
- network surface policy and authorized CIDRs
- drain mode, quarantine, and `SPF` / `DKIM` / `DMARC` controls
- Git-first update source and maintenance window

### Debian installation

The `Debian Trixie` scripts for `LPE-CT`:

- install system prerequisites
- clone the Git repository into `/opt/lpe-ct/src`
- build the `lpe-ct` binary
- create the `/var/spool/lpe-ct` spool
- deploy the static interface and `nginx` configuration
- install and restart `lpe-ct.service`

The systemd service grants `CAP_NET_BIND_SERVICE` so the binary can listen on SMTP port `25` without running the service as root.

### Product coherence

This split keeps:

- the core `LPE` services on the `LAN` for business data and `JMAP`
- the sorting center in the `DMZ` for exposable traffic
- a future local AI path without data leaving the infrastructure

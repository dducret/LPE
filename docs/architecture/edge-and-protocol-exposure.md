# Edge and Protocol Exposure | Exposition perimetrique et protocoles

## Francais

### Objectif

Ce document decrit la frontiere entre `LPE` et `LPE-CT` pour l'exposition reseau, la publication des protocoles et le transport interne.

### Regle centrale

`LPE-CT` est l'unique point d'exposition externe.

Le coeur `LPE` ne doit pas etre accessible directement depuis Internet et n'en a pas besoin pour fonctionner dans l'architecture cible.

### Exposition externe de base

`LPE-CT` publie:

- `SMTP` entrant sur le port `25`
- le client web `LPE` en `HTTPS` sur `443` sous `/mail`
- `ActiveSync` en `HTTPS` sous `/activesync`
- les endpoints `JMAP` en `TLS` vers `LPE`
- `IMAPS`
- `SMTPS`

Pour la soumission cliente securisee, la base cible prefere le port `465` en TLS implicite, conformement a `RFC 8314`.

Les WebSockets `JMAP` securisees sont une extension future et ne font pas partie de la base actuelle.

### Separation entre publication et logique protocolaire

- `LPE` porte la logique protocolaire mailbox et collaboration
- `LPE-CT` porte l'exposition externe, le reverse proxy, le proxy TCP/TLS et la posture perimetrique

### ActiveSync

`ActiveSync` est bavard et utilise frequemment du long polling.

Le frontal `LPE-CT` doit donc supporter:

- des timeouts longs
- une gestion de connexion adaptee
- l'absence de coupure prematuree pour Outlook et iOS pendant les attentes longues

### LPE-CT aussi stateless que possible

`LPE-CT` doit rester aussi stateless que possible afin de faciliter:

- le load-balancing `DNS`
- `VRRP`
- le remplacement horizontal de noeuds

L'etat edge necessaire, comme le spool ou la quarantaine, doit rester borne, explicite et operationnellement remplacable.

### Transport interne `LPE-CT <-> LPE`

Le protocole cible pour les echanges internes entre `LPE-CT` et `LPE` est `gRPC`.

Ce choix est strictement limite au backbone interne et ne change pas les protocoles exposes aux clients.

L'implementation Rust privilegiee pour cette couche est `tonic`.

Le contrat fonctionnel v1 actuellement en place reste documente separement dans `docs/architecture/lpe-ct-integration.md`.

## English

### Goal

This document describes the boundary between `LPE` and `LPE-CT` for network exposure, protocol publication, and internal transport.

### Core rule

`LPE-CT` is the unique external exposure point.

The core `LPE` server must not be directly reachable from the public Internet and does not need to be exposed for the target architecture to work.

### Baseline external exposure

`LPE-CT` publishes:

- inbound `SMTP` on port `25`
- the `LPE` web client over `HTTPS` on `443` under `/mail`
- `ActiveSync` over `HTTPS` under `/activesync`
- exposed `JMAP` endpoints over `TLS` toward `LPE`
- `IMAPS`
- `SMTPS`

For secure client submission, the baseline target prefers implicit TLS on port `465`, aligned with `RFC 8314`.

Secure `JMAP` WebSockets are a future extension and are not part of the current baseline.

### Separation between publication and protocol logic

- `LPE` owns mailbox and collaboration protocol logic
- `LPE-CT` owns external exposure, reverse proxying, TCP/TLS proxying, and edge security posture

### ActiveSync

`ActiveSync` is chatty and commonly relies on long-polling behavior.

The `LPE-CT` front layer must therefore support:

- long timeouts
- protocol-aware connection handling
- no premature disconnects for Outlook and iOS during long-held sync waits

### LPE-CT as stateless as possible

`LPE-CT` should remain as stateless as possible in order to simplify:

- `DNS` load balancing
- `VRRP`
- horizontal node replacement

Necessary edge state such as spool or quarantine must remain bounded, explicit, and operationally replaceable.

### Internal transport `LPE-CT <-> LPE`

The target protocol for internal `LPE-CT` to `LPE` exchanges is `gRPC`.

This choice is strictly limited to the internal backbone and does not change the externally exposed client protocols.

The preferred Rust implementation for that internal layer is `tonic`.

The current functional v1 contract remains documented separately in `docs/architecture/lpe-ct-integration.md`.

# Admin Federated Auth MVP | MVP d'authentification federée admin

## Francais

### Objectif

Ce document decrit la premiere brique d'authentification federée et d'`IAM` moderne pour le back office `LPE`.

Le scope concerne uniquement le plan d'administration coeur `LPE`.

### Objectifs du MVP

- ajouter un login `OIDC` sans casser l'authentification actuelle par mot de passe
- conserver la separation des roles admin serveur / tenant / domaine / support / compliance / operations
- structurer les permissions d'administration avec des identifiants normalises
- preparer la base de donnees pour des facteurs additionnels comme `TOTP`

### Hors scope du MVP

- federation des comptes mailbox utilisateur
- provisioning automatique d'administrateurs depuis l'`IdP`
- synchronisation de groupes ou de roles depuis l'`IdP`
- validation locale avancee des `ID Token`
- `PKCE` obligatoire
- enforcement `MFA`
- UX d'enrolement `TOTP`

### Flux actuel

Le flux actuel est un `OIDC` authorization code flow confidentiel:

1. l'administrateur ouvre la console
2. `LPE` expose des metadonnees `OIDC` minimales au frontend
3. le frontend demande une URL d'autorisation a l'API
4. l'API signe un `state` contenant l'origine publique et un horodatage
5. l'utilisateur s'authentifie chez l'`IdP`
6. l'`IdP` redirige vers le callback `LPE`
7. `LPE` echange le `code` contre un jeton d'acces
8. `LPE` recupere les claims via `userinfo`
9. `LPE` rattache l'identite federée a un administrateur existant
10. `LPE` cree une session admin interne avec la methode `oidc`

### Regles d'autorisation

Le fournisseur `OIDC` authentifie l'identite. L'autorisation reste entierement dans `LPE`.

Les regles actuelles sont:

- un login federé doit correspondre a un administrateur `LPE` existant
- si l'auto-link est desactive, seul un rattachement deja enregistre `issuer + subject` est accepte
- si l'auto-link est active, `LPE` peut rattacher automatiquement un identifiant federé a un administrateur deja cree ayant la meme adresse email
- aucun administrateur n'est cree automatiquement depuis l'`IdP`
- les roles et permissions `LPE` restent la source de verite

### Modele de permissions

Le MVP introduit une normalisation des permissions admin:

- chaque administrateur conserve un role principal
- les permissions sont stockees sous forme structuree
- les roles integres fournissent un ensemble de permissions par defaut
- des permissions explicites peuvent etre ajoutees pour des cas d'exception

Exemples de permissions normalisees:

- `dashboard`
- `domains`
- `accounts`
- `aliases`
- `admins`
- `policies`
- `security`
- `audit`
- `mail`
- `operations`
- `protocols`

Le role `server-admin` conserve la permission globale `*`.

### Preparation MFA

Le schema reserve maintenant:

- la methode d'authentification de la session admin
- une table dediee aux facteurs d'authentification admin

Cette base servira plus tard a:

- `TOTP`
- recovery codes
- politiques de step-up
- exigence differenciee selon le role ou la permission

### Contraintes de securite actuelles

- le mot de passe admin local reste supporte pour bootstrap et recuperation
- le callback `OIDC` doit etre aligne avec l'origine publique reelle
- les secrets `OIDC` restent configures cote serveur
- le flux repose sur `userinfo` pour le MVP et devra etre durci avant une generalisation plus large

### Licences et dependances

Le MVP n'introduit pas de bibliotheque `OIDC` additionnelle.

L'implementation reutilise des dependances deja presentes dans le workspace, ce qui evite d'ajouter une exception de licence supplementaire pour cette premiere brique.

## English

### Goal

This document describes the first federated-authentication and modern `IAM` building block for the `LPE` administration back office.

The scope only covers the core `LPE` administration plane.

### MVP goals

- add `OIDC` login without breaking the current password-based authentication
- preserve the separation between server-admin / tenant / domain / support / compliance / operations roles
- structure administration permissions around normalized identifiers
- prepare the database for additional factors such as `TOTP`

### Out of scope for the MVP

- federation for end-user mailbox accounts
- automatic administrator provisioning from the `IdP`
- group or role synchronization from the `IdP`
- advanced local `ID Token` validation
- mandatory `PKCE`
- `MFA` enforcement
- `TOTP` enrollment UX

### Current flow

The current flow is a confidential `OIDC` authorization code flow:

1. the administrator opens the console
2. `LPE` exposes minimal `OIDC` metadata to the frontend
3. the frontend asks the API for an authorization URL
4. the API signs a `state` containing the public origin and a timestamp
5. the user authenticates with the `IdP`
6. the `IdP` redirects back to the `LPE` callback
7. `LPE` exchanges the `code` for an access token
8. `LPE` fetches claims from `userinfo`
9. `LPE` binds the federated identity to an existing administrator
10. `LPE` creates an internal admin session with auth method `oidc`

### Authorization rules

The `OIDC` provider authenticates the identity. Authorization stays entirely inside `LPE`.

The current rules are:

- a federated login must match an existing `LPE` administrator
- when auto-link is disabled, only an already registered `issuer + subject` mapping is accepted
- when auto-link is enabled, `LPE` may automatically bind a federated identity to an already created administrator with the same email address
- no administrator is automatically created from the `IdP`
- `LPE` roles and permissions remain the source of truth

### Permission model

The MVP introduces normalized admin permissions:

- each administrator still has a primary role
- permissions are stored in structured form
- built-in roles provide default permission sets
- explicit permissions may be added for exceptional delegations

Examples of normalized permissions are:

- `dashboard`
- `domains`
- `accounts`
- `aliases`
- `admins`
- `policies`
- `security`
- `audit`
- `mail`
- `operations`
- `protocols`

The `server-admin` role keeps the global `*` permission.

### MFA preparation

The schema now reserves:

- the authentication method used by an admin session
- a dedicated table for administrator authentication factors

This foundation is intended for later:

- `TOTP`
- recovery codes
- step-up policies
- differentiated requirements by role or permission

### Current security constraints

- local admin password login remains supported for bootstrap and recovery
- the `OIDC` callback must match the real public origin
- `OIDC` secrets stay server-side
- the flow currently relies on `userinfo` for the MVP and should be hardened before broader rollout

### Licensing and dependencies

The MVP does not introduce an additional `OIDC` library.

The implementation reuses dependencies that are already present in the workspace, which avoids adding another license exception for this first building block.

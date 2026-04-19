# Admin Federated Auth MVP | MVP d'authentification federée admin

## Francais

### Objectif

Ce document decrit le MVP actuel d'authentification admin moderne pour `LPE`.

Le scope reste borne au plan d'administration coeur `LPE`.

### Objectifs du MVP

- supporter un login admin local robuste
- supporter un login `OIDC` admin mieux integre
- conserver l'autorisation strictement interne a `LPE`
- garder les roles et permissions admin structures
- fournir un premier `TOTP` admin exploitable pour le login mot de passe
- journaliser les connexions admin et le cycle de vie des facteurs

### Hors scope du MVP

- federation des comptes mailbox utilisateur
- provisioning automatique des administrateurs depuis l'`IdP`
- synchronisation des groupes ou roles depuis l'`IdP`
- validation locale avancee des `ID Token`
- `PKCE` obligatoire
- enforcement `MFA` generalise pour tous les admins
- recovery codes
- `TOTP` ajoute au flux `OIDC`

### Flux `OIDC`

Le flux `OIDC` reste un `authorization code flow` confidentiel:

1. l'administrateur ouvre la console
2. le frontend demande l'URL d'autorisation a l'API
3. l'API signe un `state` avec callback attendu et horodatage
4. `LPE` utilise les endpoints configures ou resout `/.well-known/openid-configuration` a partir de l'issuer si seuls l'issuer et les secrets sont fournis
5. l'utilisateur s'authentifie chez l'`IdP`
6. l'`IdP` redirige vers le callback `LPE`
7. `LPE` echange le `code` contre un jeton d'acces
8. `LPE` recupere les claims via `userinfo`
9. `LPE` rattache l'identite federée a un administrateur existant
10. `LPE` cree une session admin interne avec la methode `oidc`

### Flux mot de passe admin

Le flux mot de passe admin est maintenant:

1. l'administrateur soumet email et mot de passe
2. `LPE` verifie le credential local `argon2`
3. si un facteur `TOTP` actif existe pour cet administrateur, un code `TOTP` valide est exige
4. `LPE` cree une session admin interne avec la methode `password` ou `password+totp`
5. `LPE` journalise succes et echec dans l'audit interne

### Regles d'autorisation

Le fournisseur `OIDC` authentifie l'identite. L'autorisation reste entierement dans `LPE`.

Les regles du MVP sont:

- un login federé doit correspondre a un administrateur `LPE` existant
- si l'auto-link est desactive, seul un rattachement deja enregistre `issuer + subject` est accepte
- si l'auto-link est active, `LPE` peut rattacher automatiquement une identite federée a un administrateur deja cree ayant la meme adresse email
- aucun administrateur n'est cree automatiquement depuis l'`IdP`
- les roles et permissions `LPE` restent la source de verite

### `TOTP` admin MVP

Le MVP implemente maintenant:

- enrolement `TOTP` admin par API
- verification initiale du facteur avant activation
- verification du code au login mot de passe quand un facteur actif existe
- revocation d'un facteur admin
- journalisation des evenements d'enrolement, verification, revocation et login

### Limites actuelles

- pas de recovery codes
- pas de `step-up`
- pas d'enforcement `TOTP` sur le flux `OIDC`
- le secret `TOTP` est stocke tel quel dans la base MVP et doit etre traite comme une donnee sensible

### Contraintes de securite

- le mot de passe admin local reste supporte pour bootstrap et recuperation
- le bootstrap local peut utiliser `admin@example.test` avec `ChangeMeNow$` pour le premier acces operationnel
- ce secret de bootstrap doit etre remplace immediatement en environnement reel
- le callback `OIDC` doit etre aligne avec l'origine publique reelle
- les secrets `OIDC` restent configures cote serveur
- le flux `OIDC` repose encore sur `userinfo` pour ce MVP

### Licences et dependances

Le MVP n'introduit pas de bibliotheque `OIDC` additionnelle.

L'implementation reutilise les dependances deja presentes dans le workspace. Le `TOTP` est calcule sans ajouter une nouvelle dependance de federation.

## English

### Goal

This document describes the current modern admin-authentication MVP for `LPE`.

The scope remains limited to the core `LPE` administration plane.

### MVP goals

- support a robust local admin login
- support a better integrated admin `OIDC` login
- keep authorization strictly internal to `LPE`
- preserve structured admin roles and permissions
- provide a first usable admin `TOTP` flow for password login
- audit admin sign-ins and factor lifecycle actions

### Out of scope for the MVP

- federation for end-user mailbox accounts
- automatic administrator provisioning from the `IdP`
- group or role synchronization from the `IdP`
- advanced local `ID Token` validation
- mandatory `PKCE`
- broad mandatory `MFA` enforcement for all admins
- recovery codes
- `TOTP` layered on top of `OIDC`

### `OIDC` flow

The `OIDC` flow remains a confidential authorization code flow:

1. the administrator opens the console
2. the frontend asks the API for the authorization URL
3. the API signs a `state` with the expected callback and timestamp
4. `LPE` uses configured endpoints or resolves `/.well-known/openid-configuration` from the issuer when only issuer metadata and secrets are provided
5. the user authenticates with the `IdP`
6. the `IdP` redirects back to the `LPE` callback
7. `LPE` exchanges the `code` for an access token
8. `LPE` fetches claims from `userinfo`
9. `LPE` binds the federated identity to an existing administrator
10. `LPE` creates an internal admin session with auth method `oidc`

### Admin password flow

The admin password flow is now:

1. the administrator submits email and password
2. `LPE` verifies the local `argon2` credential
3. when an active admin `TOTP` factor exists, a valid `TOTP` code is required
4. `LPE` creates an internal admin session with auth method `password` or `password+totp`
5. `LPE` records success and failure in the internal audit log

### Authorization rules

The `OIDC` provider authenticates the identity. Authorization remains entirely inside `LPE`.

The MVP rules are:

- a federated login must match an existing `LPE` administrator
- when auto-link is disabled, only an already registered `issuer + subject` mapping is accepted
- when auto-link is enabled, `LPE` may automatically bind a federated identity to an already created administrator with the same email address
- no administrator is automatically created from the `IdP`
- `LPE` roles and permissions remain the source of truth

### Admin `TOTP` MVP

The MVP now implements:

- admin `TOTP` enrollment through the API
- initial factor verification before activation
- `TOTP` verification during password login when an active factor exists
- factor revocation
- audit logging for enrollment, verification, revocation, and sign-in events

### Current limits

- no recovery codes
- no step-up policies
- no `TOTP` enforcement on the `OIDC` flow
- the MVP stores the `TOTP` secret as-is in the database and treats it as sensitive data

### Security constraints

- local admin password login remains supported for bootstrap and recovery
- bootstrap may use `admin@example.test` with `ChangeMeNow$` for the first operational sign-in
- that bootstrap secret must be changed immediately in real deployments
- the `OIDC` callback must match the real public origin
- `OIDC` secrets remain server-side
- the current `OIDC` flow still relies on `userinfo` for this MVP

### Licensing and dependencies

The MVP does not introduce an additional `OIDC` library.

The implementation reuses dependencies already present in the workspace. `TOTP` is computed without introducing a new federation dependency.

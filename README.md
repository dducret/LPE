# La Poste ELectronique

`LPE` est une plateforme de messagerie et de collaboration moderne, multiplateforme, ecrite majoritairement en Rust.

## Principes initiaux

- code projet sous licence `Apache-2.0`
- dependances `MIT` autorisees uniquement si aucune alternative `Apache-2.0` raisonnable n'existe
- `PostgreSQL` comme stockage primaire de metadonnees
- `JMAP` comme axe principal du produit moderne
- `IMAP` et `SMTP` comme couches de compatibilite
- architecture preparee pour une IA locale future sans sortie de donnees hors serveur

## Structure

- `crates/` services et bibliotheques Rust
- `web/admin` back office React/TypeScript
- `web/client` client web type Outlook Web
- `docs/architecture` decisions techniques initiales
- `docs/licensing` politique de licences et garde-fous CI

## Demarrage

Le squelette actuel permet de compiler les crates Rust du workspace.

```powershell
cargo check
```

## Axe IA locale

`LPE` prepare des projections documentaires canoniques, des chunks et des annotations qui serviront plus tard a une integration LLM locale type Gemma, sans rendre le moteur IA dependance du coeur metier.

## Formats documentaires v1

Les pieces jointes suivantes sont prevues pour l'indexation texte en v1:

- `PDF` via `pdf_oxide`
- `DOCX` via `docx-lite`
- `ODT` via un extracteur ODF minimal focalise texte

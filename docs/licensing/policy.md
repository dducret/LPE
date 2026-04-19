# Politique de licences | License policy

## Francais

### Regle de base

- tout code source produit dans `LPE` doit etre publie sous `Apache-2.0`
- les dependances `MIT` sont autorisees uniquement lorsqu'aucune alternative `Apache-2.0` raisonnable n'existe
- les dependances `GPL`, `LGPL`, `AGPL`, `SSPL` et licences non standard sont interdites

### Procedure d'ajout d'une dependance

1. verifier la licence declaree
2. rechercher une alternative `Apache-2.0`
3. documenter la justification si une dependance `MIT` est retenue
4. ajouter un controle automatique en CI des licences

### Liste initiale des exceptions MIT a justifier

- `tokio`
- `axum`
- `multer` (transitive dependency enabled by `axum` multipart upload support)
- `tracing`
- `docx-lite`

Ces dependances sont courantes dans l'ecosysteme Rust et retenues provisoirement pour accelerer le demarrage. Une revue documentaire devra etre maintenue pour chaque exception.

`argon2` est retenu pour le hachage des mots de passe administrateur; la crate RustCrypto est disponible sous double licence `Apache-2.0 OR MIT`, donc compatible avec la preference `Apache-2.0`.

## English

### Base rule

- all source code produced in `LPE` must be published under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `GPL`, `LGPL`, `AGPL`, `SSPL`, and non-standard licenses are forbidden

### Dependency addition procedure

1. verify the declared license
2. look for an `Apache-2.0` alternative
3. document the justification if an `MIT` dependency is retained
4. add automated license checks in CI

### Initial MIT exceptions to justify

- `tokio`
- `axum`
- `multer` (transitive dependency enabled by `axum` multipart upload support)
- `tracing`
- `docx-lite`
- `tonic` for internal `LPE-CT <-> LPE` `gRPC` contracts only

These dependencies are common in the Rust ecosystem and are kept provisionally to accelerate the bootstrap. A documented review must be maintained for each exception.

`argon2` is used for administrator password hashing; the RustCrypto crate is available under `Apache-2.0 OR MIT`, which is compatible with the `Apache-2.0` preference.

Google `Magika` is an acceptable candidate for file-type validation because its official project is published under `Apache-2.0`. As with every dependency, the selected integration path and its transitive dependencies must still be reviewed before adoption.

`tonic` is accepted as a documented `MIT` exception for internal `gRPC` exchanges between `LPE-CT` and `LPE`. This exception is limited to the internal service backbone and does not change the rule that externally exposed product protocols remain those defined by the platform architecture.


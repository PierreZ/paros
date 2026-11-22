# paros-authz-biscuit

Biscuit tokens for paros: root keys, offline mint and derive, and the verifier of each request with one Datalog policy. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). **It is back in the tree since 2026-11-21**, at the same path, without `src/frontend.rs` and `tests/frontend.rs` (the adapter to the old frontend); this recap describes the crate at the tag. At the tag it lived in `crates/support/paros-authz-biscuit/` (1,844 lines of Rust).

## What it did

A support crate, `publish = false` (it pinned `biscuit-auth = "=7.0.0-alpha.3"`). It depended on `paros` only for the frontend's `Authz` trait (`paros::roles::frontend::Authz`) and `paros::model::fleet::RootKeyEntry`. Biscuit never entered `paros` or `paros-core` (its wasm32 clock needs JavaScript). Users: `parosctl` (`key`, `token`, `--frontends`), `parosd` (frontend role and the `--root-public-key` bootstrap pin) and `paros-sim`.

paros is its own issuer. A universe has Ed25519 root key pairs (`RootKey`, `KeyRing`). `mint` signs an authority block that names a `Role` (`admin` or `tenant`) and a `Grant`. Any holder narrows a token offline with `derive` and a `Restriction` (macaroon style). `seal` stops more derivation. `inspect` prints a token. The verifier (`authorize`) checks size, blocks and signature, adds request facts (time, operation, class, access, target tenant, kind, journal), and runs `POLICY`. The meaning of a role lives in the server, so a role changes with no new tokens. `BiscuitAuthz` adapts this to the frontend: it maps `frontend::Operation` to `Operation`, prefers the universe entry's keys over the pin (#245 (Biscuit tokens)), and maps `Refusal` to `Denial`.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/support/paros-authz-biscuit/src/verify.rs` | `POLICY`, `authorize`, `Request`, `TargetKind`, `Refusal` (`InvalidToken`, `Expired`, `Forbidden`), `MAX_TOKEN_BYTES` (4096), `MAX_BLOCKS` (8), `limits()`. |
| `crates/support/paros-authz-biscuit/src/token.rs` | `Role`, `Grant`, `Restriction`, `Token`, `mint`, `derive`, `seal`, `inspect`. |
| `crates/support/paros-authz-biscuit/src/keys.rs` | `RootKey`, `RootPublicKey`, `KeyRing`; JSON key files with `key_id` and `label`. |
| `crates/support/paros-authz-biscuit/src/operation.rs` | `Operation`, `Class` (`Data`, `Journal`, `TenantView`, `TenantAdmin`, `CellView`, `UniverseAdmin`), `Access`. |
| `crates/support/paros-authz-biscuit/src/entropy.rs` | `Entropy`: 32 caller bytes that seed a `ChaCha20Rng`. |
| `crates/support/paros-authz-biscuit/src/frontend.rs` | `BiscuitAuthz`, `operation`, `since_epoch`. |
| `crates/support/paros-authz-biscuit/clippy.toml` | Bans the thread-RNG key and block constructors and `AuthorizerBuilder::time`. |
| `crates/support/paros-authz-biscuit/tests/policy.rs`, `tests/frontend.rs` | The policy role by role, derivation, limits, determinism; the `Authz` adapter. |

## Ideas worth reusing

- **Deterministic crypto through `Entropy`** (`src/entropy.rs`, `clippy.toml`). Every key and block key comes from bytes the caller draws from its provider. The simulation replays every token. The `clippy.toml` `disallowed-methods` list makes the rule a lint, not a hope.
- **Time is a fact the caller adds** (`verify.rs`, `request_facts`). No `SystemTime::now()` in the verifier, so decisions replay.
- **Bounded evaluation by counts, not wall time** (`verify.rs`, `limits()`): `max_facts` 1000, `max_iterations` 100, `max_time` set to a day (not `Duration::MAX`, which overflows).
- **Refusal judged by kind only** (`Refusal`). The engine's check order is not stable, so the code never reads which check failed first. `expired` looks at the set of failed checks.
- **Restrict by class, not by operation** (`operation.rs`). A token stays valid when a new operation joins a class. Never rename a class or operation name.
- **Caller values are Datalog parameters, never source text** (`token.rs`, `check_with`): `a_name_with_quotes_stays_a_name`.
- **Policy in the server, roles in the token.** Small, easy to change.

Leaks and mistakes not to copy:

- The crate depends on the whole `paros` umbrella for one trait and one model type (`src/frontend.rs`). A narrow trait crate would do.
- It pins an alpha of `biscuit-auth` by exact version; check the current release before reuse.
- The operation list in `frontend.rs` (`FormCell`, `Admit`, `SetTags`, `Resolve`) follows the old control plane; the target design (`docs/architecture/03-5-frontend-and-resolver.md`) may change it.

## Bugs and scenarios to cover again

- **A derived token never does more than its parent**: `a_derived_token_never_does_more_than_its_parent`, `each_restriction_alone_derives_and_narrows` (`tests/policy.rs`).
- **A tenant token reaches its own tenant only; internal tenants are admin only**: `a_tenant_token_reaches_its_own_tenant_only`, `a_tenant_token_reaches_its_own_journals_only`, `an_idle_machine_call_needs_an_admin_token` (`tests/frontend.rs`).
- **Expiry on the verifier's clock, refused as `Expired`**: `an_expired_token_is_refused_as_expired`.
- **Key rotation and which keys rule**: `an_admin_token_reaches_internal_journals_and_a_rotated_in_key_verifies`, `the_universe_entry_keys_rule_over_the_pin`.
- **Size and block limits, at and past the edge**: `a_token_with_too_many_blocks_is_invalid`, `a_token_over_the_byte_limit_is_invalid`, `a_token_of_exactly_the_byte_limit_passes`.
- **Sealed tokens cannot be derived; a foreign key is invalid**: `a_sealed_token_cannot_be_derived`, `another_universe_key_is_an_invalid_token`.
- **Determinism**: `the_same_entropy_gives_the_same_token`, `a_derived_block_holds_its_checks_in_one_order`.
- **No key material in debug output**: `debug_output_never_shows_key_material`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/support/paros-authz-biscuit/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/support/paros-authz-biscuit

Where to start reading: `src/lib.rs` (the rules), `src/verify.rs`, `src/token.rs`, `src/entropy.rs` with `clippy.toml`, then `tests/policy.rs`. Its own map at the tag: `crates/support/paros-authz-biscuit/AGENTS.md`.

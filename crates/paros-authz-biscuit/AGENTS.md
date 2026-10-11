# paros-authz-biscuit

Biscuit tokens for paros (#245 Biscuit tokens, #400 keygen and offline tokens), `publish = false`
because `biscuit-auth` is pinned by git rev until 6.1. The design is `docs/architecture.md` §3.5
"Tokens are Biscuits". Used by `parosctl` (`key`, `token`, `--frontends`), the `paros-frontend`
binary and `paros-sim` (#192 (the frontend)); `parosd` joins with #245. It depends on `paros` for
the `Authz` trait only; Biscuit never enters `paros` or `paros-core`.

## Map

- `src/entropy.rs` → `Entropy`: 32 bytes the caller draws from its provider's random source,
  the seed of a `ChaCha20Rng`. moonpool's `RandomProvider` is not a `CryptoRng`, so this is the
  bridge, the same code in production and simulation.
- `src/keys.rs` → `RootKey`, `RootPublicKey`, `KeyRing`: generation, the JSON key files
  (`key_id`, `label`, the key in Biscuit's text form), the ring a verifier trusts.
- `src/operation.rs` → `Operation`, `Class`, `Access`: the operation table. Never rename an
  operation or a class: tokens in the field name them.
- `src/token.rs` → `Role`, `Grant`, `Restriction`, `Token`, `mint`, `derive`, `seal`, `inspect`:
  offline. Every caller value enters Datalog as a parameter, never as source text.
- `src/verify.rs` → `POLICY`, `Request`, `TargetKind`, `Refusal`, `authorize`: the request facts,
  the one policy, the limits, the refusal kinds.
- `src/frontend.rs` → `BiscuitAuthz`, `operation`, `since_epoch`: `paros::frontend::Authz` over
  a `KeyRing` (#192 (the frontend)); a call's names and its frontend's clock become a `Request`,
  a `Refusal` becomes a `Denial`.
- `tests/frontend.rs` → `BiscuitAuthz` through the `Authz` trait: a tenant's rights, `admin` on an
  internal journal, the rotated-in key, expiry on the frontend's clock.
- `tests/policy.rs` → the policy role by role and restriction by restriction, determinism (same
  entropy, same bytes), sealing, a foreign key, a name with quotes.

## Rules

- Two roles: `admin` and `tenant` (Pierre, 2026-10-10). Tenants and journals by name, never hex.
- #245 rules 1 to 4: keys and block keys from `Entropy` only (`clippy.toml` bans the thread-RNG
  defaults and `AuthorizerBuilder::time`); time is a fact the caller adds; `max_time` out of
  reach, `max_facts` and `max_iterations` bound the run; a refusal is judged by kind only.
- Out of scope: third-party blocks, revocation ids, P-256, snapshots, queries.

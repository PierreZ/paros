# paros-authz-biscuit

Biscuit tokens for paros (#245 (Biscuit tokens), #400 (keygen and offline tokens)): root keys,
offline minting and derivation, and the verifier of each request. `publish = false` while
`biscuit-auth` is pinned to an alpha. The design is
`docs/architecture/03-5-frontend-and-resolver.md` §3.5 "Tokens are Biscuits".

The crate stands alone. It depends on no other paros crate, it uses no runtime, and no crate of
the workspace depends on it yet. It is in layer 6 of `scripts/layers.sh`, with the adapters:
it implements the `Authz` port over an external crate (`biscuit-auth`, which draws on
`getrandom`), so it is not a pure crate of `scripts/check-core-deps.sh`, and it is not part of
the `paros` facade. Biscuit's wasm32 clock needs JavaScript, so the crate does not build for
`wasm32-unknown-unknown` (a finding of #1411 (CI checks that hold the layering)).

It came back from the tag `v0.2.0-alpha.1` on 2026-11-21 without its adapter to the old
frontend (`src/frontend.rs`: `BiscuitAuthz`, `operation`, `since_epoch`). A new frontend writes
its own adapter: it builds a `Request` for each call and calls `authorize`. The recap of the
old crate is `docs/archive/v0.2.0-alpha.1/paros-authz-biscuit.md`.

## Map

- `src/entropy.rs` → `Entropy`: 32 bytes the caller draws from its random source, the seed of a
  `ChaCha20Rng`. The same code runs in production and in a simulation.
- `src/keys.rs` → `RootKey`, `RootPublicKey`, `KeyRing`: generation, the JSON key files
  (`key_id`, `label`, the key in Biscuit's text form), the ring a verifier trusts.
- `src/operation.rs` → `Operation`, `Class`, `Access`: the operation table. Never rename an
  operation or a class: a token names them. The table is the one of the tag, so it names calls
  of the old control plane (`cell.form`, `machine.admit`, `universe.key.add`). **Open**: the
  design of the new `parosd` decides which operations stay.
- `src/token.rs` → `Role`, `Grant`, `Restriction`, `Token`, `mint`, `derive`, `seal`, `inspect`:
  offline. Every caller value enters Datalog as a parameter, never as source text.
- `src/verify.rs` → `POLICY`, `Request`, `TargetKind`, `Refusal`, `authorize`: the request facts,
  the one policy, the limits, the refusal kinds.
- `tests/policy.rs` → the policy role by role and restriction by restriction, determinism (same
  entropy, same bytes), sealing, a foreign key, a name with quotes, the size and block limits.

## Rules

- Two roles: `admin` and `tenant` (Pierre, 2026-10-10). Tenants and journals by name, never hex.
- #245 rules 1 to 4: keys and block keys from `Entropy` only (`clippy.toml` bans the thread-RNG
  defaults and `AuthorizerBuilder::time`); time is a fact the caller adds; `max_time` out of
  reach, `max_facts` and `max_iterations` bound the run; a refusal is judged by kind only.
- `clippy.toml` repeats the workspace's bans: clippy reads the nearest `clippy.toml` only. Keep
  the two files in step.
- Out of scope: third-party blocks, revocation ids, P-256, snapshots, queries.

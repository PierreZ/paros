+++
title = "Tokens and root keys"
description = "Sign tokens offline, pin the machines, and rotate the root keys of a universe"
weight = 5
+++

paros checks a caller with a Biscuit token. paros is its own issuer: a universe has one or more
root key pairs, and each token is signed with one of them. `parosctl` makes keys and tokens
offline, with no running universe.

## Make a key and a token

```sh
parosctl key generate --out keys --label prod-1
parosctl token mint --key keys/prod-1.private --role admin --ttl 1h \
  --subject operator --out admin.token
parosctl token mint --key keys/prod-1.private --role tenant --tenant acme --ttl 1h \
  --subject orders-service --out acme.token
```

The `.private` file has mode 0600: keep it secret. The `.public` file is the half that every
process checks a token with. There are two roles:

- An `admin` token administers the universe: `init`, cells, machines and tenants.
- A `tenant` token reads and writes the journals of one tenant, by name.

Any holder can make a narrower token, with no key:

```sh
parosctl token derive --token-file acme.token --read-only --journal orders
```

## Pin the machines

A new machine waits for an operator. Without a pin, it trusts any caller that the network lets
through. Give it the public key, and it trusts only an `admin` token:

```sh
export PAROS_ROOT_PUBLIC_KEY=keys/prod-1.public
parosd
```

To pass the key without a file, put the text of the `.public` file in
`PAROS_ROOT_PUBLIC_KEY_JSON`. `parosctl
init` takes the universe's first key the same way, in `PAROSCTL_ROOT_PUBLIC_KEY_JSON` or
`--root-public-key-json`.

While it is idle, a pinned machine checks the token of each call that puts it in a cell:
`cell init` and `cell add-machine`. A call without an `admin` token is refused with
`invalid_token`, `expired` or `forbidden`. A founding member that was down when `cell init` sent
the plan learns it later from a formed member. That call carries the operator's token too, so the
pinned machine checks it. The formed member keeps the token in memory only. After a restart, it
holds no token and sends nothing: run `parosctl init` again with your token.

```sh
parosctl --token-file admin.token init --root-public-key keys/prod-1.public
parosctl --token-file admin.token cell add-machine parosd-4:4500
```

`init --root-public-key` writes the key in the universe. After this step, the universe holds
the keys that a frontend uses.

## The keys of the universe

| command | what it does |
|---|---|
| `parosctl universe key list` | prints the keys in force, by label |
| `parosctl universe key add <file>` | adds the key of a `.public` file |
| `parosctl universe key remove <label>` | removes a key; the last key stays |

A frontend reads the keys with the universe directory. When a token is refused as
`invalid_token`, the frontend reads the keys again and checks the token once more. Thus a new
key is trusted at its first use. A removed key stops being trusted at the next read. Before the
first read, the frontend uses the keys of `PAROS_ROOT_PUBLIC_KEY`, if there are any.

A rotation has three steps:

1. Add the next key: `parosctl universe key add keys/prod-2.public`.
2. Sign new tokens with the next key.
3. When the tokens of the old key expired, remove it: `parosctl universe key remove prod-1`.

A key id and a label are never used again. Tokens are short-lived, and there is no revocation:
to stop a token, remove its key.

## Limits

- A machine pins its key at its start. To change the pin, restart it with the new
  `PAROS_ROOT_PUBLIC_KEY`.
- The cell coordinator finishes an admission that an operator left in flight. It has no token,
  so a pinned machine refuses it. Run `cell add-machine` again with your token.

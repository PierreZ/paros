---
layout: ../../layouts/Doc.astro
kicker: parosd
section: parosd/
status: parosd
title: Names and tokens
description: The person says what, the system says where. People type names; machines carry ids that the service draws and checks. An id is an address, never an authority.
---

## The problem

A person thinks in names: the tenant `acme`, the journal `billing/invoices`. A protocol needs
small fixed ids. If the person must pick the id, two people pick the same one. If an id is also
the right to use the journal, a caller that guesses the right number gets in.

## The rule

**Names for people, ids for machines, and an id is never an authority** (goal 5 of
[§1.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/01-goal.md#11-the-goals-and-the-root-rules),
[§3.8](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-8-identifiers-checkpoints-recovery-security.md#38-identifiers)).
A person types and reads ASCII names. Machines and protocols carry `u64` ids. The right to call
comes from a token and the names in it, at the frontend.

<div class="arch" role="img" aria-label="A client sends names and a Biscuit token to the frontend. The frontend checks the token and changes the names into ids. Past the frontend, only ids travel to the roles of the journal.">
  <div class="box">client <small>paros://acme/billing/invoices · a Biscuit token</small></div>
  <div class="arrow">▼ <small>names and a token, never an id</small></div>
  <div class="box lead">frontend <small>checks the token · names → ids · forwards</small></div>
  <div class="arrow">▼ <small>ids only</small></div>
  <div class="box">the roles of the journal</div>
</div>

## Names

A user addresses a journal as `paros://<tenant>/<namespace>/<journal>`. The namespace is the
placement key with the tenant, and the unit of a move between cells
([§3.5](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-5-frontend-and-resolver.md#35-the-frontend-and-the-resolver)).
Only the entry roles resolve names: the resolver the tenant, the frontend the journal. A client
never reads the directory, so no tenant sees the names of another tenant. Everything a person
reads or types names an entity by its name; a short hex id appears only when no name is known.

## Ids: drawn by the service, checked at apply

A client does not draw an id. A create call carries a name and a request id. The service draws
the id, and the fold checks it at apply and refuses an id that is in use:

| Id | Checked by | Unique in |
|---|---|---|
| `TenantId` | the fold of the universe | the universe |
| `JournalId` | the fold of the tenant | its tenant |

A counter in the fold was refused: the tenant of a cell exists before the cell joins a universe,
so no counter of the universe can give its id. A name is free again once its delete completes,
and a new tenant or journal with that name draws a new id, so an old id never names the new one.
**Open**: the role that draws, the genesis records of the first tenants, and a birth nonce in
`Assign` (#1419 (the tenancy wrapper)).

## An id is an address

A client has no field to put an id in. So a caller that crafts the right `u64` gains nothing:
it can only reach the frontend, and the frontend asks for a token. A reference of moonpool RPC
is not a credential either; the fences stay with the terms and the ballots.

The core knows no tenant. `paros-consensus` and `paros-journal` carry one journal id. The pair
(tenant, journal) is made above the core, by the tenancy crate that wraps it (#1402 (remove the
tenant from the core)).

## Biscuit tokens at the frontend

paros is its own issuer: a root key pair, whose public half is in the universe entry. A token is
a Biscuit, short-lived, with no revocation. `parosctl` mints tokens offline, with no running
universe. Two roles: `admin` for the universe and everything below, and `tenant` for the data
of one tenant. A token names its tenant and journals by name, never by id. Any holder narrows a
token offline (read-only, one namespace, one journal, an earlier expiry), with no key.

The frontend is a role like the others ([Layers](../layers/)). The operator gives it to machines;
a client finds the frontends of its cell through `Resolve`. It checks the token, changes names
into ids and forwards each call: it never redirects the client to a journal. The nodes behind it
do no authorization: the boundary is the network.

## In paros

`paros-authz-biscuit` is in the tree today: root keys, offline mint and derive, and the verifier
of a request with one Datalog policy. It stands alone and no crate uses it yet. The frontend
role and its adapter to Biscuit are #1431 (the frontend role, the resolver and the Biscuit
adapter).

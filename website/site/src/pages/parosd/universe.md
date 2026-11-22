---
layout: ../../layouts/Doc.astro
kicker: parosd
section: parosd/
status: parosd
title: The universe
description: Cells sit in a universe. An override table, then a hash and a bucket table, say which cell holds a tenant. A move between cells is one bucket row.
---

## The problem

One cell is one blast radius: a bad deploy, an overload or a poison pill stops at its edge. So
paros has many cells, and each tenant lives in one of them. Something must say which cell, and
answer fast for every client. If that answer is a list of every tenant, the list is on the path
of every new tenant and of every move.

## The rule

**The override table first, then the hash and the bucket table** (goal 6 of
[§1.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/01-goal.md#11-the-goals-and-the-root-rules),
[§3.7](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-7-universe.md#37-the-universe),
the hash slots of Redis Cluster):

1. **The override table.** A tenant created with a named cell, or with a tag selector, has an
   override entry, with a reason for people (`dedicated`, `canary`, `quarantine`). An override
   names one tenant exactly, and it always wins.
2. **The bucket.** Every other tenant goes to a bucket: `bucket = CRC16(key) mod 16384`
   (CRC16-XMODEM), where the key is the tenant and the namespace.
3. **The bucket table.** A fixed table of 16384 rows, `bucket → cell`, names the cell.

<div class="arch" role="img" aria-label="A tenant name goes first to the override table. If it has an entry, that entry names the cell. If not, CRC16 of the key modulo 16384 gives a bucket, and the bucket table gives the cell of that bucket.">
  <div class="box">tenant + namespace <small>a name that a person typed</small></div>
  <div class="arrow">▼</div>
  <div class="box side">override table <small>one row per tenant · a reason · always wins</small></div>
  <div class="arrow">▼ <small>no row</small></div>
  <div class="box">CRC16(key) mod 16384 <small>a bucket, the same for every reader</small></div>
  <div class="arrow">▼</div>
  <div class="cellframe">
    <span class="cellname">bucket table</span>
    <div class="row three">
      <div class="box">0 – 8191 <small>cell paris</small></div>
      <div class="box">8192 – 12287 <small>cell lyon</small></div>
      <div class="box">12288 – 16383 <small>cell lille</small></div>
    </div>
  </div>
</div>

The cell names and the ranges are an example. The hash is a fixed function of the key, so every
reader that folds the same directory places a name in the same cell. When `paros-universe`
exists, this page gets a live box: type a name, see its bucket and its cell, from the real hash
in wasm (#1440 (a lab for each pure layer)).

## A move is one bucket row

The first cell takes every bucket. A new cell gets no bucket, so it fills on purpose, never by
surprise. `universe buckets assign` gives a range of buckets to a cell. A tenant placed by an
override never blocks a bucket move, and a cell with no bucket is a dedicated cell.

A move is one row of the bucket table: the row gets the state `Moving { from, to }`, and each
namespace of that bucket moves with no change of protocol (#1433 (the move of a tenant between
cells)). That state and the steps of the move are **Open** in §3.7. The universe tenant and the cell tenants are never placed by a bucket, so
a bucket move never moves them.

## The directory

The universe is one more level of the same shape ([One shape at every level](../control/)): a
control journal, the **directory**, a pure fold and one coordinator, which runs in the
coordinator of the home cell. The directory holds the cells, the buckets, the overrides and one
registration record per tenant. That record is one write at creation, never on the path of a
request, and it is where the fold checks the `TenantId` ([Names and tokens](../names-and-tokens/)).

The resolver is the thin router: one pool per region, stateless, serving from the last directory
it read. A client asks it for the cell of its tenant, then asks that cell for its frontends. The
resolver keeps answering while the universe is down. Only new tenants, new cells and moves wait.

## In paros

The hash, the two tables and their fold are pure and belong to the universe layer (#1432
(paros-universe)). The move between cells is not proved yet: it gets its own model before its
code.

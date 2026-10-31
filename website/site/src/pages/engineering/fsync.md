---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: When fsync fails
description: A failed sync can leave bytes that look written and are not on the device. paros drops the page cache at every store open and reads what the device holds.
---

## The problem

An acceptor syncs its vote, then answers. What if the sync fails?

On Linux, a failed writeback can mark the dirty pages **clean** without writing them. A retried
`fsync` then succeeds, because nothing is dirty any more. The page cache still serves the bytes,
also to the next process that opens the file. Then a power loss drops them. This is
"fsyncgate": PostgreSQL found it in 2018.

So "retry the sync" is wrong, and "crash and restart" is not enough: the restarted process reads
the same cached pages and trusts them.

## What paros does

<div class="seq">
<table>
<thead><tr><th>Step</th><th>store</th><th>page cache</th><th>device</th><th class="note">What paros does</th></tr></thead>
<tbody>
<tr><td>1</td><td class="msg">write vote</td><td>dirty</td><td></td><td class="note">The vote is not acknowledged yet.</td></tr>
<tr><td>2</td><td class="msg">sync ✗</td><td>clean (a lie)</td><td>old bytes</td><td class="note">The sync fails. The <code>Accepted</code> does not leave.</td></tr>
<tr><td>3</td><td>quarantine</td><td></td><td></td><td class="note">A storage fault takes its journal out of service, not the whole process.</td></tr>
<tr><td>4</td><td class="msg">re-open</td><td>dropped</td><td></td><td class="note">Every open first drops the clean cached pages of the store's files.</td></tr>
<tr><td>5</td><td class="msg">read</td><td></td><td>old bytes</td><td class="note">The open reads what the device holds. A missing vote is a missing vote.</td></tr>
<tr><td>6</td><td>repair</td><td></td><td></td><td class="note">The journal gets a lost or ambiguous slot back from its peers.</td></tr>
</tbody>
</table>
</div>

- **Drop, do not rewrite.** The open never writes a byte. A rewrite would make the stranded
  pages durable, but a crash in the middle of it can tear records that were durable.
- **Do not sync at open.** A dirty page that a failed commit left still reads as written and
  lands at the next sync. A sync at each open could fail and take the open with it.
- **A one-sync commit syncs again.** A `Batched` store whose sync fails writes the batch again
  and syncs again, at most three times. The batch is not acknowledged yet, so this is safe.

## How it is tested

moonpool injects failed writebacks on the simulated disk, with the same seed-driven schedule as
every other fault. The fault family runs unmasked in every simulation run. The `hint!`
"journal re-opened, not synced" can crash the machine right after a re-open, on what the failed
sync left. The audit checks that no acknowledged vote is lost and that a slot with no clean copy
is never accepted again.

## In paros

- `crates/lib/paros-storage/src/journal/settle.rs`: `drop_caches` and `settle`.
- `crates/lib/paros-roles/src/node.rs`: a quarantined journal re-opens from its store.
- moonpool: `StorageFault::WritebackError`, the simulated fsyncgate.

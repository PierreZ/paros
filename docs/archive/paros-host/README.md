# paros-host, removed on 2026-11-25

Pierre removed the pure host of actors `paros-host` on 2026-11-25 ("Remove Paros host too"),
when he dropped the layering above the core and the journal built again from the plan (D7 of
`docs/architecture/04-7-segments.md`). A new plan comes after the segments work. No crate of the
workspace ran its roles: the journal roles of design A ran on it, and they left on 2026-11-24.

The code is in the history of `main`: the last commit that holds it is
`bda1436d45986f0d6fe548dfd86765f3af833d7c`. Read it with
`git show bda1436d:crates/lib/paros-host/README.md`, or check that commit out.

## What it held

- `crates/lib/paros-host` (layer 0, no dependency at all), built by #1410 (split the generic host
  of actors):
  - `Family`: the types that every role of one host shares (an opaque address, a message, a
    batch id, an op of the disk format, the records of a role, an assignment, an input, an
    output, a stop).
  - `Role`: one actor. Start (at once, or from its records), step, fire (its own timer), drawn
    (a random value that it asked for), stop.
  - `Host`: the pure state machine that maps each address to its role, starts a role from its
    records, routes each input and keeps the deadline of each role's timer.
  - `Driver`: steps the host, commits the persist batches of a round in one write (group
    commit), and sends each message only once the batch that it waits on is durable (the flight
    rule).
  - `Storage`, `Transport`, `Clock`, `Entropy`: the traits that a backend implements. A storage
    lists the roles on its disk, and a fault is a typed `StorageError` (`Full`, `Lost`,
    `Unknown`) with one `Verdict` (#1414).
  - `Ports`, `Events`: where the outputs of the roles go, and the record of each step for a
    trace check.
  - The example `coordinator`: a toy coordinator with a disk and two workers with none. A crash
    and a restart give a higher term, and a late answer of the old term is refused.
- Two TLA+ models of `specs/tla/`:
  - `flight-rule/`: persist ids, a send that names the persist id that it waits on, group
    commit, a crash that drops the batches in flight, and the faults of a storage with their
    verdicts. Its `own_batch` control shows the bug when a reply waits only on the batch of its
    own step (#1255 (journal sans-IO contract)).
  - `recorded-inputs/`: every input is recorded (the time and the random draws in each input,
    one timer for each role), for #1413 (the time, the timers and the draws in each role). Not
    built in the crate.
- Its CI: the rustdoc step and the example step of the `checks` job, its row in
  `scripts/layers.sh`, its lines in `scripts/tla-minutes.txt` and `scripts/mutate-tla-shards.txt`,
  its pass of the `Mutants nightly` workflow (`scripts/mutants.sh --host`,
  `.cargo/mutants-host.toml`, its row in `scripts/mutants-plan.sh`), and its entry in
  `release-plz.toml`.
- The facade module `paros::host`.

The contract that it built is §J4.2 of `docs/journal/04-roles.md` at the git tag
`journal-design-a`.

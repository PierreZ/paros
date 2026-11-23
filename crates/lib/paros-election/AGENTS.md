# paros-election

The election over a journal (#1420 (paros-election), `docs/architecture/03-3-coordinators-and-placement.md`
§3.3, decision 13; `docs/architecture/04-roles.md` §4.4). A thin layer on the pure journal client,
`paros_journal::client`: it owns one `Client`, and the program gives it the time, the draws and the
answers of the journal. It keeps nothing on disk.

- `src/record.rs`: the election record (one client header, the term record and the renewal) and its
  codec. The walk decodes it with its own code, from the layout written there.
- `src/election.rs`: the state machine (watch, claim, fold to the tail, publish, lead, hand on, adopt);
  `src/election/tests.rs` its unit tests.
- `src/config.rs`: the knobs (lease, poll, renewal, jitter, page) and their defaults.
- `examples/three_actors.rs`: three actors over a journal in memory; one leads, it stops, another wins.

The model is `specs/tla/election/` (`Election.tla`); the walk is `crates/sim/journal-sim/src/election.rs`,
traced against `specs/tla/election/ElectionTrace.tla`. A change of a rule changes the model first.

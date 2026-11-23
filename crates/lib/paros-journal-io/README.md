# paros-journal-io

The IO side of the paros journal v1, with no runtime and no storage engine. A program runs the
pure journal roles of `paros-journal` by implementing four traits and calling a driver in a
loop:

| Trait | What a backend does |
|---|---|
| `Storage<Journal>` | One atomic `commit` of ops, and a `scan` of one role's records. The format is this crate's (`layout`): a backend stores keys and bytes |
| `Transport<Journal>` | `send` a message, `receive` the next one. Every message goes through it, between two roles of one process too |
| `Clock` | The time now. The roles read no clock: the driver gives the time with every input |
| `Register` | The journal's compare-and-set register: run a change, read the value of a done write |

`Storage`, `Transport` and `Clock` are the traits of the generic host of actors (`paros-host`),
for the journal's family `Journal`. The `Host` is the pure state machine that serves many roles
of many journals in one process, on the generic host: it routes each input to its role and turns
the role's effects into effects that name their journal. The `Driver` is the generic driver with
the journal's ports: it steps the `Host`, commits the persist batches of a round in one write (group
commit), and sends each message only once the batch that it waits on is durable (the flight
rule). The `Events` port reports each step, for the trace check and the grid controller.

No trait requires `Send`, an executor or a runtime type. The crate depends on `paros-consensus`,
`paros-journal` and `paros-host` alone. `mem` has backends in memory, written with the standard library, and
`contract` has the suites that check a backend's API.

## Examples

Each example prints a trace and asserts what it shows. Read them in this order:

    nix develop --command cargo run -p paros-journal-io --example in_memory

| Example | Shows |
|---|---|
| `in_memory` | A journal on three processes, on the backends of `mem`. A process crashes and starts again from its storage; a second segment seals the first |
| `custom_storage` | A storage backend written by hand (an append-only log with a checksum per batch), checked by the contract suite, with a journal on it. A crash cuts the last batch, and the batch is dropped whole |
| `single_writer` | The pure client of `paros_journal::client`: a writer claims the journal, a lost reply and its resend answered `Duplicate`, a lost reply across a seal answered `Below` and settled by `Resolve`, then a deposed writer refused `Fenced` |
| `multi_writer` | Multi-writer mode with the client: `SetLeader` of a multi-writer mode with a fresh tag, open writes from two writers sent at once and ordered by the journal, a lost reply whose resend lands twice (at-least-once), then back to one writer |

The rules are in `docs/journal/` of the repository (§J4.2, §J4.3).

# paros-host

A pure host of actors, with no runtime and no storage engine. A role is an actor: a
deterministic step that reads no clock, draws no random number and does no I/O. Each step
returns its effects: a persist batch with its id, a send that names the batch it waits on, and
an output that names the batch it waits on (the flight rule). One host serves every role of a
process.

| Item | What it is |
|---|---|
| `Family` | The types that every role of one host shares: an opaque address, a message, a batch id, an op of the disk format, the records of a role, an assignment, an input, an output, a stop |
| `Role` | One actor: start (at once, or from its records), step, fire (its own timer), drawn (a random value it asked), stop |
| `Host` | The pure state machine that maps each address to its role, starts a role from its records, routes each input and keeps the deadline of each role's timer |
| `Driver` | Steps the host, commits the persist batches of a round in one write (group commit), and sends each message only once the batch that it waits on is durable |
| `Storage`, `Transport`, `Clock`, `Entropy` | The traits a backend implements, for one family; `Entropy` only for roles that draw |
| `Ports`, `Events` | Where the outputs of the roles go, and the record of each step for a trace check |

No trait requires `Send`, an executor or a runtime type, and the crate depends on no crate.
The journal of paros (`paros-journal-io`) runs its acceptors, replicas, leaders and proxy
leaders on it.

## Example

    nix develop --command cargo run -p paros-host --example coordinator

A toy coordinator with a disk and two workers with none. The coordinator writes a new term at
each start, and every order it sends waits for that write. A crash and a restart give a higher
term, and a late answer of the old term is refused.

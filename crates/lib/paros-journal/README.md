# paros-journal

The pure core of the paros journal v1: one ordered log for each journal, with Paxos inside each
segment. It holds the journal fold, the `Resolve` call, the held answers, the segment records,
the journal's matchmaker register, the journal roles (acceptor, leader, segment replica) and the
codec.

The crate is a library that a program uses through plain data and pure state machines. It does no
I/O, reads no clock and draws no random number. It depends on `paros-consensus` alone, with no
async runtime and no storage engine. The program that runs the roles, the driver, brings the
storage, the transport and the clock. The rules are in `docs/journal/` of the repository.

## Examples

Each example prints a trace and asserts what it shows. Read them in this order:

    nix develop --command cargo run -p paros-journal --example fold

| Example | Shows |
|---|---|
| `fold` | The journal fold alone: `SetLeader`, `Write` with `expected_seq`, a resend answered `Duplicate`, `Taken`, `Early`, `Fenced`, `Truncate`, a seal, a resend answered `Below` and its `Resolve`, multi-writer mode |
| `in_memory` | A whole journal in one process: three acceptors, two leaders and two segments, driven by a loop written by hand. The storage is a key-value map in memory with an adapter for each role, and the driver keeps the flight rule. An acceptor crashes and starts again from its store, and a second segment seals the first |

The examples depend on this crate alone: CI checks that their build pulls in no crate from
outside the workspace (`scripts/check-core-deps.sh`).

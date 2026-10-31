# docs

Research and design notes backing the Paxos implementation. No code here.

- `knowledge/` — the Paxos knowledge base: one card per concept paros uses, each backed by a
  paper or a real implementation and mapped onto the code. Start here for a protocol
  question; `knowledge/CLAUDE.md` is its map and `knowledge/concepts.md` its index.
- `references/papers/<name>/` — source paper (`paper.pdf`) plus a markdown `transcript.md`.
  Read the transcript first; it's searchable and citable.
- `references/<repo>/` — analysis of an external *code* implementation we study
  (e.g. `references/frankenpaxos/` reads Whittaker's codebase for sans-IO Paxos patterns).
- `references/talks/<name>.md` — transcript of a talk or video (no PDF), headed by its source URL.
- `analysis/` — our own design notes derived from the papers and other implementations
  (e.g. `analysis/go-raft/` studies etcd's sans-IO architecture for Multi-Paxos).

When adding a paper, keep the `paper.pdf` + `transcript.md` pair. When adding analysis,
cite the transcript/section it draws from.

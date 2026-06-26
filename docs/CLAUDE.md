# docs

Research and design notes backing the Paxos implementation. No code here.

- `references/papers/<name>/` — source paper (`paper.pdf`) plus a markdown `transcript.md`.
  Read the transcript first; it's searchable and citable.
- `references/<repo>/` — analysis of an external *code* implementation we study
  (e.g. `references/frankenpaxos/` reads Whittaker's codebase for sans-IO Paxos patterns).
- `references/talks/<name>.md` — transcript of a talk or video (no PDF), headed by its source URL.
- `analysis/` — our own design notes derived from the papers and other implementations
  (e.g. `analysis/go-raft/` studies etcd's sans-IO architecture for Multi-Paxos).

When adding a paper, keep the `paper.pdf` + `transcript.md` pair. When adding analysis,
cite the transcript/section it draws from.

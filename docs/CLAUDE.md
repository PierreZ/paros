# docs

Research and design notes backing the Paxos implementation. No code here.

- `references/papers/<name>/` — source paper (`paper.pdf`) plus a markdown `transcript.md`.
  Read the transcript first; it's searchable and citable.
- `analysis/` — our own design notes derived from the papers and other implementations
  (e.g. `analysis/go-raft/` studies etcd's sans-IO architecture for Multi-Paxos).

When adding a paper, keep the `paper.pdf` + `transcript.md` pair. When adding analysis,
cite the transcript/section it draws from.

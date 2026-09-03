# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### 🚜 Refactor

- **core**: `RawNode` is renamed `ColocatedNode`. The type is not a "raw"
  anything: it is the deployment that colocates the `Acceptor`, `Proposer`
  and `Replica` roles on one node and wires them together. The driver half of
  the etcd-raft split it mirrors keeps its name, `paros::run_node`, so only
  the type and its methods are affected — a caller renames the type and
  nothing else.

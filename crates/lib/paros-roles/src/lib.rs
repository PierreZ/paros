//! The roles layer: one module per role of the papers, each with its
//! `run_*` loop. The node is the colocated leader, acceptor and learner;
//! the proxy leader (§3.1), the replica (§3.3) and the batcher (§4.1, §4.2)
//! are the compartmentalized roles of *Scaling Replicated State Machines
//! with Compartmentalization*; the matchmaker is Matchmaker Paxos's; the
//! frontend is paros's per-tenant edge.

pub mod batcher;
pub mod frontend;
pub mod matchmaker;
pub mod node;
pub mod proxy_leader;
pub mod replica;

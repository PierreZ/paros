----------------------------- MODULE ReadsTrace -----------------------------
(***************************************************************************)
(* The trace spec of the leaderless reads of `Reads`, on a walk of the     *)
(* `consensus-sim` cluster with matchmakers and reads (#1231 (traced walks *)
(* reach the rules),                                                       *)
(* `crates/sim/consensus-sim/src/cluster/trace/reads.rs`).                 *)
(*                                                                         *)
(* The log, the matchmaking and the GC are `MatchmakerTrace` (linked from  *)
(* `../matchmaker/`): this module extends it and adds the read steps. The  *)
(* read steps are the rules of `Reads.tla`, written for the many reads of  *)
(* a walk (`Reads` has one read) over the log of `MatchmakerTrace`:        *)
(*   - "basis": a node learns the read basis of a leadership that won      *)
(*     its ballot (`LearnBasis`, Basis = "won"), with that leadership's    *)
(*     fence: one below the frontier of its Phase 1, which covers every    *)
(*     slot that the Phase 1 recovered (`FenceOf`). The node's promise is  *)
(*     not above the ballot (`Below`, Learner = "strict"). The basis only  *)
(*     moves forward;                                                      *)
(*   - "readstart": a node opens a read on its basis (`ReadStart`). Its    *)
(*     promise is not above the basis (`Below`). It remembers the writes   *)
(*     that completed before it (`snap`). A reader of the configuration    *)
(*     counts its own watermark at once;                                   *)
(*   - "readack": an acceptor answers with its vote watermark and its read *)
(*     fence, the higher of its promise and its belief ballot (`AckFence`, *)
(*     Fence = "promise"). The trace has no belief, so the step checks the *)
(*     promise part: the fence is at or above the promise;                 *)
(*   - "readfold": the reader folds an answer it was sent (`PreReadAck`).  *)
(*     An answer whose fence is above the basis ends the read. Any other   *)
(*     counts while the answers hold no Phase-1 quorum yet;                *)
(*   - "readserve": the answers hold a Phase-1 quorum of the basis         *)
(*     configuration, the promise is still not above the basis, and the    *)
(*     reader learned every slot up to the index, which is the maximum of  *)
(*     the watermarks and the fence (`ReadServe`);                         *)
(*   - "applied": a party applied every slot up to `slot`: those writes    *)
(*     completed (`Ack`). Some node learned each slot up to it, so each    *)
(*     is chosen (`LogLearnedChosen`).                                     *)
(* A read that the code drops (its TTL, a promise above its basis) takes   *)
(* no step: it stays open here and serves nothing. A restart forgets the   *)
(* read basis and the open reads of the node (`Restart` of `Reads`).       *)
(*                                                                         *)
(* `ReadSeesAcked` is the property of `Reads`: a served read covers each   *)
(* write that completed before the read started. Watermarks, fences and    *)
(* indexes that are none are -1.                                           *)
(***************************************************************************)
EXTENDS MatchmakerTrace, Integers

VARIABLES rwon,   \* the ballots whose Phase 1 won
          rbasis, \* node: the ballot (0: none) and the fence of its read basis
          racks,  \* the answers the acceptors sent
          rds,    \* read token: the read
          acked   \* the highest slot whose write completed (-1: none)

readVars == <<rwon, rbasis, racks, rds, acked>>

Ctxs == TraceCtxs
NoBasis == [bal |-> 0, fence |-> -1]
NoRead == [state |-> "none", node |-> None, b |-> 0, fence |-> -1, snap |-> -1,
           answers |-> {}, index |-> -1]

MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

\* The vote watermark of acceptor a: its highest slot with a record.
VoteMark(a) == MaxOf({s \in Slots : lvote[a][s].bal > 0} \cup {-1})

ReadsInit ==
  /\ TraceInit
  /\ rwon = {}
  /\ rbasis = [n \in Acceptors |-> NoBasis]
  /\ racks = {}
  /\ rds = [c \in Ctxs |-> NoRead]
  /\ acked = -1

IsCtx == Has("ctx") /\ e.ctx \in Ctxs

-----------------------------------------------------------------------------
(* The read steps *)

RBasis ==
  /\ e.event = "basis" /\ IsNode("node") /\ IsBallot /\ Has("fence")
  /\ LET n == e.node
         b == e.bal
     IN /\ b \in rwon
        /\ e.fence = lfront[b] - 1
        /\ lprom[n] <= b
        /\ rbasis[n].bal <= b
        /\ rbasis' = [rbasis EXCEPT ![n] = [bal |-> b, fence |-> e.fence]]
  /\ UNCHANGED <<rwon, racks, rds, acked>>

RReadStart ==
  /\ e.event = "readstart" /\ IsNode("node") /\ IsBallot /\ IsCtx /\ Has("fence")
  /\ LET n == e.node
         b == e.bal
     IN /\ rds[e.ctx].state = "none"
        /\ rbasis[n] = [bal |-> b, fence |-> e.fence]
        /\ lprom[n] <= b
        /\ Has("own") <=> n \in cfg[b]
        /\ Has("own") => e.own = VoteMark(n)
        /\ rds' = [rds EXCEPT ![e.ctx] =
                     [state |-> "open", node |-> n, b |-> b, fence |-> e.fence, snap |-> acked,
                      answers |-> IF Has("own") THEN {[acc |-> n, wm |-> e.own]} ELSE {},
                      index |-> -1]]
  /\ UNCHANGED <<rwon, rbasis, racks, acked>>

RReadAck ==
  /\ e.event = "readack" /\ IsNode("acc") /\ Has("ctx") /\ Has("wm") /\ Has("fence")
  /\ e.wm = VoteMark(e.acc)
  /\ e.fence >= lprom[e.acc]
  /\ racks' = racks \cup {[acc |-> e.acc, ctx |-> e.ctx, wm |-> e.wm, fence |-> e.fence]}
  /\ UNCHANGED <<rwon, rbasis, rds, acked>>

\* The answers of read r hold a Phase-1 quorum of its configuration.
Whole(r) == \E Q \in Maj(cfg[r.b]) : Q \subseteq {x.acc : x \in r.answers}

RReadFold ==
  /\ e.event = "readfold" /\ IsNode("node") /\ IsNode("acc") /\ IsCtx
  /\ LET r == rds[e.ctx] IN
     /\ r.state = "open"
     /\ r.node = e.node
     /\ e.acc \in cfg[r.b]
     /\ \E k \in racks :
          /\ k.acc = e.acc /\ k.ctx = e.ctx
          /\ IF k.fence > r.b
             THEN rds' = [rds EXCEPT ![e.ctx].state = "ended"]
             ELSE /\ ~ Whole(r)
                  /\ ~ \E x \in r.answers : x.acc = e.acc
                  /\ rds' = [rds EXCEPT ![e.ctx].answers = @ \cup {[acc |-> e.acc, wm |-> k.wm]}]
  /\ UNCHANGED <<rwon, rbasis, racks, acked>>

RReadServe ==
  /\ e.event = "readserve" /\ IsNode("node") /\ IsCtx /\ Has("index")
  /\ LET r == rds[e.ctx]
         i == MaxOf({x.wm : x \in r.answers} \cup {r.fence})
     IN /\ r.state = "open"
        /\ r.node = e.node
        /\ lprom[e.node] <= r.b
        /\ Whole(r)
        /\ e.index = i
        /\ \A s \in Slots : s <= i => learned[e.node][s] # None
        /\ rds' = [rds EXCEPT ![e.ctx].state = "done", ![e.ctx].index = i]
  /\ UNCHANGED <<rwon, rbasis, racks, acked>>

RApplied ==
  /\ e.event = "applied" /\ IsSlot
  /\ e.slot > acked
  /\ \A s \in Slots : s <= e.slot => \E n \in Acceptors : learned[n][s] # None
  /\ acked' = e.slot
  /\ UNCHANGED <<rwon, rbasis, racks, rds>>

ReadStep ==
  /\ \/ RBasis \/ RReadStart \/ RReadAck \/ RReadFold \/ RReadServe \/ RApplied
  /\ UNCHANGED <<vars, logVars>>

\* A win marks its ballot won; a restart forgets the read basis and the open
\* reads of its node. Every other step of `MatchmakerTrace` leaves the reads.
LogStep ==
  /\ Step
  /\ rwon' = IF e.event = "win" THEN rwon \cup {e.bal} ELSE rwon
  /\ rbasis' = IF e.event = "restart" THEN [rbasis EXCEPT ![e.node] = NoBasis] ELSE rbasis
  /\ rds' = IF e.event = "restart"
            THEN [c \in Ctxs |-> IF rds[c].state = "open" /\ rds[c].node = e.node
                                 THEN [rds[c] EXCEPT !.state = "ended"] ELSE rds[c]]
            ELSE rds
  /\ UNCHANGED <<racks, acked>>

ReadsNext == l <= Len(Trace) /\ (LogStep \/ ReadStep) /\ l' = l + 1

ReadsSpec == ReadsInit /\ [][ReadsNext]_<<l, vars, logVars, readVars>>

-----------------------------------------------------------------------------
(* Safety *)

\* A served read covers each write that completed before it started.
ReadSeesAcked == \A c \in Ctxs : rds[c].state = "done" => rds[c].snap <= rds[c].index
=============================================================================

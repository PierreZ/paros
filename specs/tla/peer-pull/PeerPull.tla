------------------------------ MODULE PeerPull --------------------------------
(***************************************************************************)
(* The pull of a lagging replica from a peer of its segment (the first     *)
(* half of R8 of `docs/architecture/04-7-segments.md`, step 6 of the       *)
(* segments in the core). The acceptors dropped segment k at its `Hold`:   *)
(* its peers hold it (R7). The lagging replica `Lag` asks the leader on    *)
(* each beat and gets a trim point of a later segment that does not      *)
(* prove the segment released. It must pull the segment from a peer of   *)
(* its set.                                                                *)
(*                                                                         *)
(* The safety of a copy from a peer (a peer holds only chosen slots of its *)
(* segment) is `CanCopy` of `held-floor/HeldFloor.tla`. This model checks  *)
(* liveness: the replica holds its segment in the end. A set may name     *)
(* pooled nodes beside replicas (a `Roll` names any nodes): a node of the  *)
(* set dropped the segment with the other acceptors, and answers a pull    *)
(* with a trim point, as the leader does. Replicas of the set may crash,   *)
(* and drop the pulls that reach them while down.                          *)
(*                                                                         *)
(* Constant (with a control):                                              *)
(*   PullRule "replicas": each trim point makes the replica pull from the  *)
(*            next replica of its set, in turn; the nodes of the set are   *)
(*            left out, since the beats ask them already. A replica peer   *)
(*            never answers with a trim point, so no pull draws a pull.    *)
(*            "guard": the control, the first rule written: each trim      *)
(*            point from a party outside the set makes the replica pull    *)
(*            from the next peer of the set, nodes included, in turn. When *)
(*            the leader is in the set, no trim point ever pulls.         *)
(*            "first": a control, each trim point pulls from the first    *)
(*            replica of the set, which may not hold the segment.          *)
(*   Beats    TRUE; FALSE in the control `no_beat`, where no trim point   *)
(*            comes and so no pull leaves.                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Lag,        \* the lagging replica
  Leader,     \* the node that leads and beats, in the pool
  NodePeers,  \* the pooled nodes that the set of the segment names
  RepPeers,   \* the other replicas that the set names, as a sequence
  Holders,    \* the replicas of RepPeers that hold the whole segment
  Crashes,    \* how many times the replicas of RepPeers may crash
  Beats,      \* the leader beats (FALSE only in the control `no_beat`)
  PullRule

ASSUME PullRule \in {"replicas", "guard", "first"}
ASSUME Beats \in BOOLEAN
ASSUME Holders \subseteq {RepPeers[i] : i \in 1..Len(RepPeers)}
ASSUME Holders # {}

Reps == {RepPeers[i] : i \in 1..Len(RepPeers)}

\* Every peer of the set, in the order the replica asks them: the nodes of
\* the set (in any fixed order), then the replicas.
NodeSeq == CHOOSE s \in [1..Cardinality(NodePeers) -> NodePeers] :
             \A i, j \in DOMAIN s : i # j => s[i] # s[j]
AllPeers == NodeSeq \o RepPeers

\* The peers the rule pulls from, in turn.
Pullable == CASE PullRule = "replicas" -> RepPeers
              [] PullRule = "first" -> <<RepPeers[1]>>
              [] OTHER -> AllPeers

VARIABLES
  held,   \* the lagging replica holds its whole segment
  turn,   \* the next pull goes to Pullable[turn + 1]
  trims,  \* the senders of the trim points in flight to the replica
  pulls,  \* the peers that a pull in flight goes to
  paged,  \* a page of the segment is in flight to the replica
  up,     \* the replicas of RepPeers that run
  crashes

vars == <<held, turn, trims, pulls, paged, up, crashes>>

TypeOK ==
  /\ held \in BOOLEAN
  /\ turn \in 0..(Len(Pullable) - 1)
  /\ trims \subseteq NodePeers \cup {Leader}
  /\ pulls \subseteq NodePeers \cup Reps
  /\ paged \in BOOLEAN
  /\ up \subseteq Reps
  /\ crashes \in 0..Crashes

Init ==
  /\ held = FALSE
  /\ turn = 0
  /\ trims = {}
  /\ pulls = {}
  /\ paged = FALSE
  /\ up = Reps
  /\ crashes = 0

\* The leader beats; the lagging replica asks it, and it answers with a
\* trim point of a later segment (it dropped the segment). With `Beats`
\* off, no trim point ever comes (the control `no_beat`).
Beat ==
  /\ Beats
  /\ trims' = trims \cup {Leader}
  /\ UNCHANGED <<held, turn, pulls, paged, up, crashes>>

\* The lagging replica steps a trim point from `from`.
StepTrim(from) ==
  /\ from \in trims
  /\ trims' = trims \ {from}
  /\ LET pull == ~held /\ (PullRule = "guard" => from \notin NodePeers)
         to == Pullable[turn + 1]
     IN IF pull
          THEN /\ pulls' = pulls \cup {to}
               /\ turn' = (turn + 1) % Len(Pullable)
          ELSE UNCHANGED <<pulls, turn>>
  /\ UNCHANGED <<held, paged, up, crashes>>

\* A peer steps a pull: a node answers with a trim point, a replica that
\* runs and holds the segment answers with a page, any other replica drops
\* it.
StepPull(to) ==
  /\ to \in pulls
  /\ pulls' = pulls \ {to}
  /\ trims' = IF to \in NodePeers THEN trims \cup {to} ELSE trims
  /\ paged' = (paged \/ (to \in up /\ to \in Holders))
  /\ UNCHANGED <<held, turn, up, crashes>>

\* The lagging replica steps a page: it holds its segment and seals.
StepPage ==
  /\ paged
  /\ held' = TRUE
  /\ paged' = FALSE
  /\ UNCHANGED <<turn, trims, pulls, up, crashes>>

\* A replica of the set crashes (a crash of one that is down only spends
\* the budget).
Crash(r) ==
  /\ crashes < Crashes
  /\ up' = up \ {r}
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<held, turn, trims, pulls, paged>>

Recover(r) ==
  /\ up' = up \cup {r}
  /\ UNCHANGED <<held, turn, trims, pulls, paged, crashes>>

Next ==
  \/ Beat
  \/ \E f \in NodePeers \cup {Leader} : StepTrim(f)
  \/ \E p \in NodePeers \cup Reps : StepPull(p)
  \/ StepPage
  \/ \E r \in Reps : Crash(r) \/ Recover(r)

\* The beats go on, every message is stepped, and a crashed replica runs
\* again.
Spec ==
  /\ Init /\ [][Next]_vars
  /\ WF_vars(Beat)
  /\ \A f \in NodePeers \cup {Leader} : WF_vars(StepTrim(f))
  /\ \A p \in NodePeers \cup Reps : WF_vars(StepPull(p))
  /\ WF_vars(StepPage)
  /\ \A r \in Reps : WF_vars(Recover(r))

\* The lagging replica holds its segment in the end.
Heals == <>held
=============================================================================

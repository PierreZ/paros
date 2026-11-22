------------------------------ MODULE Degraded ------------------------------
(***************************************************************************)
(* A reshape when a column of the grid is below quorum, for #1258 (reshape *)
(* with a column below quorum). The journal grid is                        *)
(* `Grid { rows: 2F + 1, cols: P, acks: F + 1 }` (#1257 (quorum           *)
(* everywhere)). A column is below quorum when fewer than `F + 1` of its   *)
(* acceptors are live. Then no Phase-1 quorum of the grid exists (Phase 1  *)
(* needs `F + 1` of every column), but a write still completes in another  *)
(* column (#533 (move a stuck round)).                                     *)
(*                                                                         *)
(* The model is the Phase-1 bypass of `zero-stall/Bypass.tla` in the mode  *)
(* "carry" with the bypass check on, with grid quorums from `Grid.tla`,    *)
(* acceptors that crash, the seal end of design A (#1213 (segments         *)
(* experiment)) and leaderless reads that a client sees. A leader that     *)
(* leads at `b` in the old grid moves to its next ballot `b2` in a new     *)
(* grid with no Phase 1.                                                   *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Layout     configuration -> [seq, cols, acks]: its members in sorted  *)
(*              order (the row-major layout of `Grid.tla`) and its grid.   *)
(*   CfgOf      ballot -> its configuration. A configuration that changes  *)
(*              never comes back (one segment for each configuration).     *)
(*   CanCrash   the acceptors that may crash. A crashed acceptor takes no  *)
(*              step. The read invariants still judge every quorum, the    *)
(*              crashed members too, so they also hold when a member comes *)
(*              back with its disk.                                        *)
(*   DownAtStart TRUE: the acceptors of CanCrash are down from the start.  *)
(*              The first ballot needs no Phase 1 (no prior configuration),*)
(*              so its leader still leads in the old grid, and writes in   *)
(*              the column that is not below quorum. This is the worst     *)
(*              case for liveness and keeps the liveness checks small.     *)
(*   Hold       what a leader waits for before it acks a slot, before it   *)
(*              gives a read basis (with Gate = "ack_read") and before GC: *)
(*     "phase1"     a Phase-1 quorum of each prior configuration promised  *)
(*                  its ballot (the decision hold of #579, zero-stall).    *)
(*     "block"      the promises meet every read quorum of each prior      *)
(*                  configuration. On the grid this is `F + 1` of one      *)
(*                  column: a Phase-2 quorum of the old grid.              *)
(*     "block_short" a control: the promises meet only the read quorums    *)
(*                  with one acceptor more of every column (one promise    *)
(*                  short of "block").                                     *)
(*     "none"       no hold.                                               *)
(*   Gate       "ack": the hold gates the acks only (zero-stall today).    *)
(*              "ack_read": it gates the read basis too (the option of     *)
(*              #1270 (the hold does not order two reads)).                *)
(*              "propose": it also gates the proposals: the leader sends   *)
(*              no Accept at its ballot before its hold holds.             *)
(*   RecordCheck TRUE: a reader also asks a matchmaker majority, and it    *)
(*              retries when a matchmaker holds a registration of another  *)
(*              configuration above its basis or a GC floor above it (the  *)
(*              segment-record check of #1254 (leaderless reads across a   *)
(*              reshape)).                                                 *)
(*   EndRule    the end slot of the old segment, written once at the       *)
(*              bypass: "frontier" = 1 + the highest slot that the leader  *)
(*              proposed at `b` (its allocator frontier). "chosen" = a     *)
(*              control: 1 + the chosen prefix.                            *)
(*   Gc         TRUE: the leader of a ballot can make a GC floor at its    *)
(*              ballot effective, once each slot that it carried or        *)
(*              recovered is chosen at its ballot and its hold holds.      *)
(*                                                                         *)
(* A later ballot runs Phase 1 over every prior configuration (Matchmaker  *)
(* Paxos), but a vote at a ballot of a sealed configuration counts only    *)
(* below the end slot of its segment: above it, the slot belongs to the    *)
(* next segment (design A).                                                *)
(*                                                                         *)
(* Not in the model: matchmaker crashes (`quorum-everywhere/` has them),   *)
(* matchmaker generations, the cooperative handoff and two bypasses in a   *)
(* row (`zero-stall/` has them). A read is one atomic step: an invariant   *)
(* judges the reads that could start now, as `zero-stall/ReadSafe` does.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, Grid

CONSTANTS Layout, CfgOf, Matchmakers, Values, Slots, NB, Owner,
          CanCrash, DownAtStart, Hold, Gate, RecordCheck, EndRule, Gc

ASSUME /\ Hold \in {"phase1", "block", "block_short", "none"}
       /\ Gate \in {"ack", "ack_read", "propose"}
       /\ RecordCheck \in BOOLEAN
       /\ EndRule \in {"frontier", "chosen"}
       /\ Gc \in BOOLEAN
       /\ DownAtStart \in BOOLEAN

Ballots  == 1..NB
Configs  == DOMAIN Layout
Free     == "free"
Mem(c)   == GridMembers(Layout[c].seq)
Acceptors == UNION {Mem(c) : c \in Configs}
\* The quorums. Only the minimal quorums count: each use asks for a quorum
\* inside a set, or for a set that meets every quorum, or for a read whose
\* guard only weakens and whose index only grows with more answers. TLC
\* evaluates these definitions again at each use, so they are built from the
\* columns (exactly n members of each column, or of one column) and not by
\* a filter over every subset of the members. The ASSUME below checks once
\* that they are the minimal sets of grid/Grid.tla.
Minimal(Qs) == {Q \in Qs : \A R \in Qs : R \subseteq Q => R = Q}
ColK(c, j, n) == {T \in SUBSET GridColumn(Layout[c].seq, Layout[c].cols, j) : Cardinality(T) = n}
Cols(c) == 0..(Layout[c].cols - 1)
\* n members of every column.
EachCol(c, n) ==
    {UNION {f[j] : j \in Cols(c)} :
        f \in {g \in [Cols(c) -> UNION {ColK(c, j, n) : j \in Cols(c)}] :
                 \A j \in Cols(c) : g[j] \in ColK(c, j, n)}}
Need(c, slack) == GridPhase1Count(Layout[c].seq, Layout[c].cols, Layout[c].acks, slack)
Q1(c)    == EachCol(c, Need(c, 0))
Q2(c)    == UNION {ColK(c, j, Layout[c].acks) : j \in Cols(c)}
\* The read quorums with one acceptor more of every column, for the control
\* "block_short": a set that meets each of them can miss a real read quorum.
Q1Big(c) == IF Layout[c].acks >= 2 THEN EachCol(c, Need(c, 0) + 1) ELSE {}
MajQ     == {Q \in SUBSET Matchmakers : 2 * Cardinality(Q) > Cardinality(Matchmakers)}

ASSUME \A c \in Configs : GridAdmits(Layout[c].seq, Layout[c].cols, Layout[c].acks)
ASSUME \A c \in Configs :
         /\ Q1(c) = Minimal(GridQ1(Layout[c].seq, Layout[c].cols, Layout[c].acks, 0))
         /\ Q2(c) = Minimal(GridQ2(Layout[c].seq, Layout[c].cols, Layout[c].acks))
         /\ Layout[c].acks >= 2 =>
              Q1Big(c) = Minimal(GridQ1(Layout[c].seq, Layout[c].cols, Layout[c].acks - 1, 0))
ASSUME Hold = "block_short" => \A c \in Configs : Layout[c].acks >= 2
\* A configuration that the ballots leave never comes back.
ASSUME \A b1, b2, b3 \in Ballots :
         (b1 < b2 /\ b2 < b3 /\ CfgOf[b1] = CfgOf[b3]) => CfgOf[b2] = CfgOf[b1]

VARIABLES status,    \* ballot -> "matching" | "matched" | "leading" | "switched"
          reg,       \* matchmaker -> set of <<ballot, configuration>>
          replied,   \* ballot -> matchmakers that answered its registration
          hist,      \* ballot -> union of the histories in those answers
          maxwm,     \* ballot -> the highest watermark in the answers
          wm,        \* matchmaker -> its GC watermark
          promised,  \* acceptor -> ballot
          votes,     \* set of <<acceptor, slot, ballot, value>>
          promises,  \* set of <<acceptor, ballot, votes of the acceptor then>>
          safe,      \* ballot -> slot -> Free | the one value allowed
          proposals, \* set of <<ballot, slot, value>>
          acks,      \* set of <<ballot, slot>> that a leader acked to a client
          served,    \* slots that a leaderless read returned to a client
          down,      \* crashed acceptors
          segEnd,    \* configuration -> end slot of its segment, 0 = open
          via        \* ballot -> the ballot that its leader bypassed from, or 0

vars == <<status, reg, replied, hist, maxwm, wm, promised, votes, promises, safe,
          proposals, acks, served, down, segEnd, via>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init == /\ status = [b \in Ballots |-> "matching"]
        /\ reg = [m \in Matchmakers |-> {}]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ maxwm = [b \in Ballots |-> 0]
        /\ wm = [m \in Matchmakers |-> 0]
        /\ promised = [a \in Acceptors |-> 0]
        /\ votes = {}
        /\ promises = {}
        /\ safe = [b \in Ballots |-> [s \in Slots |-> Free]]
        /\ proposals = {}
        /\ acks = {}
        /\ served = {}
        /\ down = IF DownAtStart THEN CanCrash ELSE {}
        /\ segEnd = [c \in Configs |-> 0]
        /\ via = [b \in Ballots |-> 0]

(***************************************************************************)
(* Matchmaking (Matchmaker Paxos, section 3).                              *)
(***************************************************************************)
\* The registration of b reaches matchmaker m while b matches. A second
\* registration of b at m is refused by the check of the ballots that m
\* holds, as any registration at or below one that m holds.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ \A r \in reg[m] : r[1] < b
    /\ wm[m] <= b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, CfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ maxwm' = [maxwm EXCEPT ![b] = Max({@, wm[m]})]
    /\ UNCHANGED <<status, wm, promised, votes, promises, safe, proposals, acks, served,
                   down, segEnd, via>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ \E Q \in MajQ : Q \subseteq replied[b]
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<reg, replied, hist, maxwm, wm, promised, votes, promises, safe, proposals,
                   acks, served, down, segEnd, via>>

\* The prior configurations: the ones at or above the highest watermark.
PriorCfgs(b) == {r[2] : r \in {h \in hist[b] : h[1] >= maxwm[b]}}

Promisers(b) == {p[1] : p \in {q \in promises : q[2] = b}}

\* A Phase-1 quorum of every prior configuration promised b.
Covered(b) == \A c \in PriorCfgs(b) : \E Q \in Q1(c) : Q \subseteq Promisers(b)

\* The promises of b meet every set of Qs(c), for each prior configuration c.
Blocks(b, Qs(_)) == \A c \in PriorCfgs(b) : \A Q \in Qs(c) : Q \cap Promisers(b) # {}

HoldOK(b) ==
    CASE Hold = "phase1"      -> Covered(b)
      [] Hold = "block"       -> Blocks(b, Q1)
      [] Hold = "block_short" -> Blocks(b, Q1Big)
      [] OTHER                -> TRUE

(***************************************************************************)
(* Phase 1, Phase 2 and the bypass.                                        *)
(***************************************************************************)
\* Acceptor a promises b. The leader of b sends a Prepare in a campaign, and,
\* after a bypass, for the hold.
Promise(a, b) ==
    /\ status[b] = "matched" \/ (status[b] = "leading" /\ ~HoldOK(b))
    /\ a \in UNION {Mem(c) : c \in PriorCfgs(b)}
    /\ a \notin down
    /\ promised[a] <= b
    /\ ~ \E p \in promises : p[1] = a /\ p[2] = b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ promises' = promises \cup
          {<<a, b, {<<w[2], w[3], w[4]>> : w \in {x \in votes : x[1] = a}}>>}
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, votes, safe, proposals, acks,
                   served, down, segEnd, via>>

\* A vote at ballot bv counts at slot s only inside the segment of its
\* configuration: below the end slot once the segment is sealed.
InSegment(bv, s) == segEnd[CfgOf[bv]] = 0 \/ s < segEnd[CfgOf[bv]]

Reports(b) == UNION {p[3] : p \in {q \in promises : q[2] = b}}

SafeAt(b, s) ==
    LET R == {r \in Reports(b) : r[1] = s /\ r[2] < b /\ InSegment(r[2], s)}
    IN  IF R = {} THEN Free
        ELSE (CHOOSE r \in R : \A x \in R : x[2] <= r[2])[3]

Phase1Done(b) ==
    /\ status[b] = "matched"
    /\ Covered(b)
    /\ status' = [status EXCEPT ![b] = "leading"]
    /\ safe' = [safe EXCEPT ![b] = [s \in Slots |-> SafeAt(b, s)]]
    /\ UNCHANGED <<reg, replied, hist, maxwm, wm, promised, votes, promises, proposals, acks,
                   served, down, segEnd, via>>

Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ Gate = "propose" => HoldOK(b)
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, promised, votes, promises, safe,
                   acks, served, down, segEnd, via>>

Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in Mem(CfgOf[b])
    /\ a \notin down
    /\ promised[a] <= b
    /\ <<a, s, b, v>> \notin votes
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, promises, safe, proposals, acks,
                   served, down, segEnd, via>>

ProposedAt(b) == {p[2] : p \in {x \in proposals : x[1] = b}}
ValAt(b, s)   == (CHOOSE p \in proposals : p[1] = b /\ p[2] = s)[3]

ChosenAt(b, s) ==
    \E v \in Values : \E Q \in Q2(CfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

Chosen(s, v) ==
    \E b \in Ballots : \E Q \in Q2(CfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

IsChosen(s) == \E v \in Values : Chosen(s, v)

\* The chosen prefix: the highest k with each slot of 1..k chosen.
ChosenPrefix == Max({k \in {0} \cup Slots : \A s \in 1..k : IsChosen(s)})

\* b2 is the next ballot of the owner of b.
NextOf(b, b2) ==
    /\ b2 > b
    /\ Owner[b2] = Owner[b]
    /\ \A x \in Ballots : (x > b /\ x < b2) => Owner[x] # Owner[b]

\* The end slot of the old segment, written once at the bypass.
EndOf(b) ==
    IF EndRule = "frontier" THEN 1 + Max({0} \cup ProposedAt(b)) ELSE 1 + ChosenPrefix

\* The bypass: the leader at b moves to b2 with no Phase 1. It proposed each
\* slot that its Phase 1 recovered, and the matchmaker answers to b2 show no
\* registration strictly between b and b2. It proposes its own values again
\* at b2 ("carry"). When b2 changes the configuration, the bypass seals the
\* old segment.
Bypass(b, b2) ==
    /\ status[b] = "leading"
    /\ status[b2] = "matched"
    /\ NextOf(b, b2)
    /\ \A s \in Slots : safe[b][s] # Free => s \in ProposedAt(b)
    /\ \A r \in hist[b2] : ~ (r[1] > b /\ r[1] < b2)
    /\ status' = [status EXCEPT ![b] = "switched", ![b2] = "leading"]
    /\ safe' = [safe EXCEPT ![b2] =
          [s \in Slots |-> IF s \in ProposedAt(b) THEN ValAt(b, s) ELSE Free]]
    /\ segEnd' = IF CfgOf[b2] # CfgOf[b] /\ segEnd[CfgOf[b]] = 0
                 THEN [segEnd EXCEPT ![CfgOf[b]] = EndOf(b)]
                 ELSE segEnd
    /\ via' = [via EXCEPT ![b2] = b]
    /\ UNCHANGED <<reg, replied, hist, maxwm, wm, promised, votes, promises, proposals, acks,
                   served, down>>

(***************************************************************************)
(* GC, acks, reads and crashes.                                            *)
(***************************************************************************)
GcReady(b) ==
    /\ Gc
    /\ status[b] \in {"leading", "switched"}
    /\ \A s \in Slots : safe[b][s] # Free => ChosenAt(b, s)
    /\ HoldOK(b)

Collect(b, m) ==
    /\ GcReady(b)
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, replied, hist, maxwm, promised, votes, promises, safe, proposals,
                   acks, served, down, segEnd, via>>

\* The leader of b acks slot s to its client. A leader that switched can still
\* ack a slot that a late quorum chose at its old ballot.
Ack(b, s) ==
    /\ status[b] \in {"leading", "switched"}
    /\ <<b, s>> \notin acks
    /\ ChosenAt(b, s)
    /\ HoldOK(b)
    /\ acks' = acks \cup {<<b, s>>}
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, promised, votes, promises, safe,
                   proposals, served, down, segEnd, via>>

Acked == {x[2] : x \in acks}

\* The fence of a leadership: the highest slot that it recovered or carried.
Fence(b) == Max({0} \cup {s \in Slots : safe[b][s] # Free})

\* A reader holds the basis of a leadership that won. With Gate = "ack_read"
\* the leader gives it only once its hold holds.
BasisOK(b) ==
    /\ status[b] \in {"leading", "switched"}
    /\ Gate \in {"ack_read", "propose"} => HoldOK(b)

\* The segment-record check: a matchmaker majority shows no segment above the
\* basis (no registration of another configuration above it, no GC floor
\* above it).
RecordOK(b) ==
    \/ ~RecordCheck
    \/ \E MQ \in MajQ : \A m \in MQ :
          /\ wm[m] <= b
          /\ ~ \E r \in reg[m] : r[1] > b /\ r[2] # CfgOf[b]

\* A leaderless read on basis b at the read quorum Q (quorum_read.rs): no
\* answer shows a higher ballot, and the read index is the highest slot that
\* an answer voted in, or the fence.
ReadPasses(b, Q) == BasisOK(b) /\ RecordOK(b) /\ \A a \in Q : promised[a] <= b

ReadIdx(b, Q) == Max({Fence(b)} \cup {w[2] : w \in {x \in votes : x[1] \in Q}})

\* A read is served once each slot up to its index is chosen. `Next` takes
\* the read quorum Q from the Phase-1 quorums of the basis.
Serve(b, Q) ==
    /\ Q \cap down = {}
    /\ ReadPasses(b, Q)
    /\ \A s \in 1..ReadIdx(b, Q) : IsChosen(s)
    /\ ~ (1..ReadIdx(b, Q) \subseteq served)
    /\ served' = served \cup 1..ReadIdx(b, Q)
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, promised, votes, promises, safe,
                   proposals, acks, down, segEnd, via>>

Crash(a) ==
    /\ a \in CanCrash \ down
    /\ down' = down \cup {a}
    /\ UNCHANGED <<status, reg, replied, hist, maxwm, wm, promised, votes, promises, safe,
                   proposals, acks, served, segEnd, via>>

(***************************************************************************)
(* The steps of the protocol, which fairness covers, and the crashes and    *)
(* the reads, which it does not.                                           *)
(***************************************************************************)
Progress ==
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m) \/ Collect(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b)
    \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
    \/ \E b \in Ballots, s \in Slots, v \in Values : Propose(b, s, v)
    \/ \E a \in Acceptors, p \in proposals : Accept(a, p[1], p[2], p[3])
    \/ \E b, b2 \in Ballots : Bypass(b, b2)
    \/ \E b \in Ballots, s \in Slots : Ack(b, s)

Next ==
    \/ Progress
    \/ \E a \in Acceptors : Crash(a)
    \/ \E b \in Ballots : \E Q \in Q1(CfgOf[b]) : Serve(b, Q)

Spec == Init /\ [][Next]_vars

\* The liveness checks: the protocol keeps stepping while it can.
LiveSpec == Spec /\ WF_vars(Progress)

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)
Agreement == \A s \in Slots : \A v, w \in Values : (Chosen(s, v) /\ Chosen(s, w)) => v = w

\* No leaderless read that can start now misses a slot that a client saw:
\* a slot that a leader acked, or a slot that an earlier read returned.
ReadSafe ==
    ~ \E b \in Ballots :
        \E Q \in Q1(CfgOf[b]) :
           /\ ReadPasses(b, Q)
           /\ \E s \in Acked \cup served : s > ReadIdx(b, Q)

\* As ReadSafe, for the acked slots only (zero-stall's ReadSafe).
ReadSafeAcked ==
    ~ \E b \in Ballots :
        \E Q \in Q1(CfgOf[b]) :
           /\ ReadPasses(b, Q)
           /\ \E s \in Acked : s > ReadIdx(b, Q)

\* A ballot that ended its matching heard no GC floor above itself: a
\* matchmaker refuses a registration below its watermark
\* (`MatchRefusal::BelowWatermark` of matchmaker.rs). Without it, a ballot
\* below the floor of a later leader can match on a history that the GC
\* emptied, and lead with no prior configuration to recover from.
HeardNoFloorAbove == \A b \in Ballots : status[b] # "matching" => maxwm[b] <= b

\* Matchmaking ends before Phase 1 (Matchmaker Paxos): no acceptor promises
\* a ballot that still matches.
PrepareAfterMatch == \A p \in promises : status[p[2]] # "matching"

\* The prior configurations of a ballot do not change once it matched: its
\* Phase 1 and its bypass check read them.
HistFixed == [][\A b \in Ballots :
                  status[b] # "matching" => hist'[b] = hist[b] /\ maxwm'[b] = maxwm[b]]_vars

Bypassed == \E b \in Ballots : via[b] # 0

\* T1: after a bypass, the bypass ballot acks a slot.
AckAfterBypass == Bypassed ~> (\E b \in Ballots, s \in Slots : via[b] # 0 /\ <<b, s>> \in acks)

\* T1: after a bypass, the old configuration retires: a matchmaker majority
\* holds a GC floor at the bypass ballot.
RetireAfterBypass ==
    Bypassed ~> (\E b \in Ballots : via[b] # 0 /\ \E MQ \in MajQ : \A m \in MQ : wm[m] >= b)

\* The two liveness properties, as invariants of the end states. Each step
\* of Progress grows a set or moves a status, a watermark or a promise up,
\* so every behavior that fairness drives reaches a state where Progress is
\* not enabled, and the goals only grow. A liveness property above thus
\* holds exactly when its goal holds in each such end state that a bypass
\* reached. TLC checks these invariants much faster than the temporal
\* properties.
Stuck == ~ ENABLED Progress
AckedAfterBypass ==
    (Stuck /\ Bypassed) => \E b \in Ballots, s \in Slots : via[b] # 0 /\ <<b, s>> \in acks
RetiredAfterBypass ==
    (Stuck /\ Bypassed) =>
        \E b \in Ballots : via[b] # 0 /\ \E MQ \in MajQ : \A m \in MQ : wm[m] >= b

\* Not real invariants: TLC must find each case, to show that the model
\* reaches it.
\* A bypass while a column of the old grid is below quorum.
NoDegradedBypass ==
    ~ \E b \in Ballots :
        /\ via[b] # 0
        /\ ~ \E Q \in Q1(CfgOf[via[b]]) : Q \cap down = {}
\* The same, and the bypass ballot acked a slot.
NoDegradedAck ==
    ~ \E b \in Ballots :
        /\ via[b] # 0
        /\ ~ \E Q \in Q1(CfgOf[via[b]]) : Q \cap down = {}
        /\ \E s \in Slots : <<b, s>> \in acks
=============================================================================

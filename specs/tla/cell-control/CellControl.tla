----------------------------- MODULE CellControl -----------------------------
(***************************************************************************)
(* Rule 3 of the rework, "a small authority, one writer", and the cell    *)
(* parts of rules 4 and 5 (docs/architecture/03-3-coordinators-and-        *)
(* placement.md, 03-2-machines.md; step 1.7, #1096). A coarse model: no   *)
(* Paxos inside. A set has members, and it serves while a majority of     *)
(* them is up and reachable.                                              *)
(*                                                                         *)
(* The authority (decision 1): a coordinator term is one compare-and-set  *)
(* of the leader uuid on the cell control journal (`SetLeader(new, old)`),*)
(* with no read of a second journal. The coordinator is the one writer of *)
(* that journal. It records each action in the journal before it does it, *)
(* and each order carries its term; the receiver of an order keeps the    *)
(* highest term it saw on disk and refuses an order below it.             *)
(*                                                                         *)
(* The duties of a term: admit a machine, replace a member that is gone   *)
(* one for one (or report "cannot place" when no machine can take its     *)
(* place), write `MachineGone` once the id is in no set, and apply a      *)
(* request of the tenant level once (its idempotency id, its outcome      *)
(* record). The operator lowers the survive number and declares a machine *)
(* gone (`remove-machine --failed`, decision 24) only through records.    *)
(*                                                                         *)
(* The faults: one machine wiped for good (its disk comes back as a new   *)
(* id, decision 4), machines that restart and come back at a new address, *)
(* a receiver of orders that restarts.                                    *)
(*                                                                         *)
(* The constants that select a control (each must break its property):    *)
(*   Fence = FALSE       a coordinator writes without the uuid fence.      *)
(*   OrderFence = FALSE  a receiver takes an order of any term.            *)
(*   Durable = FALSE     a receiver forgets its highest term on a restart. *)
(*   Idem = FALSE        a coordinator looks for an outcome of its own term *)
(*                       only, as if the outcome were not in the journal.  *)
(*   AutoLower = TRUE    the cell lowers the survive number by itself.     *)
(*   Shrink = TRUE       a change keeps only the members that answered.    *)
(*   GoneCheck = FALSE   the operator declares a machine gone with no     *)
(*                       safety check.                                     *)
(*   Candidates = "bookings"  only a booked machine may campaign.          *)
(*   Repair = "inplace"  a set waits for its broken member to come back.   *)
(*   Address = "coordinator"  a new address is written by the coordinator. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Initial,      \* the machines of `init`
          NewId,        \* the id a wiped machine comes back with, or None
          Sets,         \* "cj" (the cell control journal) and data sets
          Mem0,         \* Mem0[s]: the first members of each set
          Booked,       \* the machines a coordinator booking names
          Requests,     \* requests of the tenant level, by idempotency id
          MaxTerms, MaxRestarts, Wipes,
          Eager,        \* TRUE: a candidate may depose a live coordinator
          Fence, OrderFence, Durable, Idem, AutoLower, Shrink,
          GoneCheck,
          Candidates, Repair, Address

None == "none"
Ids == Initial \cup (IF NewId = None THEN {} ELSE {NewId})
Size(s) == Cardinality(Mem0[s])

VARIABLES alive,     \* machines whose process runs
          addr,      \* machines whose current address the cell knows
          gone,      \* ids wiped for good
          registry,  \* the ids the registry names
          lead, term,\* the leader uuid of the control journal: its holder, its count
          coord,     \* coord[i]: the term i believes it holds, 0 for none
          members,   \* members[s]: the members in force of each set
          seen,      \* seen[s]: the highest order term the receiver of s keeps
          top,       \* top[s]: the highest term of an order s applied
          net,       \* orders in flight: <<s, members, term>>
          sent,      \* <<s, gone member, term>> of each recorded replacement
          outcome,   \* <<q, t>>: the outcome record of request q, in term t
          applies,   \* applies[q]: how many times q took effect
          survive, opSurvive,  \* the survive number, and its last operator record
          reported,  \* sets with a "cannot place" report
          restarts,
          stale,     \* TRUE once a write happened outside the current term
          regress    \* TRUE once a set took an order below one it took

vars == <<alive, addr, gone, registry, lead, term, coord, members, seen, top,
          net, sent, outcome, applies, survive, opSurvive, reported, restarts, stale, regress>>

Up(i) == i \in alive /\ i \in addr
Quorum(s, ok) == 2 * Cardinality(members[s] \cap ok) > Size(s)
Serves(s) == Quorum(s, {i \in Ids : Up(i)})
Holder(i) == lead = i /\ coord[i] = term /\ Up(i)
Coordinator == \E i \in Ids : Holder(i)

\* A write to the control journal: the journal must serve, and the writer
\* must hold the uuid (the fence). `stale` records a write of a deposed one.
Writer(i) ==
  /\ Up(i) /\ coord[i] > 0 /\ Serves("cj")
  /\ IF Fence THEN Holder(i) ELSE TRUE
Wrote(i) == stale' = (stale \/ coord[i] # term)

Spares(s) == {n \in registry \cap alive : n \notin gone /\ n \notin members[s]}

Init ==
  /\ alive = Initial /\ addr = Initial /\ gone = {} /\ registry = Initial
  /\ lead = (CHOOSE i \in Booked : TRUE) /\ term = 1
  /\ coord = [i \in Ids |-> IF i = lead THEN 1 ELSE 0]
  /\ members = Mem0
  /\ seen = [s \in Sets |-> 0] /\ top = [s \in Sets |-> 0]
  /\ net = {} /\ sent = {} /\ outcome = {}
  /\ applies = [q \in Requests |-> 0]
  /\ survive = 1 /\ opSurvive = 1
  /\ reported = {} /\ restarts = 0 /\ stale = FALSE /\ regress = FALSE

-----------------------------------------------------------------------------
\* The term. Any machine that serves the cell may campaign (decision 7);
\* the control: only a booked machine. A campaign waits for one lease with
\* no coordinator, unless Eager.
Campaign(i) ==
  /\ term < MaxTerms
  /\ Up(i) /\ i \in registry /\ Serves("cj")
  /\ IF Candidates = "bookings" THEN i \in Booked ELSE TRUE
  /\ Eager \/ ~Coordinator
  /\ lead' = i /\ term' = term + 1
  /\ coord' = [coord EXCEPT ![i] = term + 1]
  /\ UNCHANGED <<alive, addr, gone, registry, members, seen, top, net, sent,
                 outcome, applies, survive, opSurvive, reported, restarts, stale, regress>>

\* Replace a member that is gone, one for one: the record, then the order.
Replace(i, s, d, n) ==
  /\ Writer(i) /\ Repair = "routine"
  /\ d \in members[s] \cap gone /\ n \in Spares(s)
  /\ <<s, d, coord[i]>> \notin sent
  /\ sent' = sent \cup {<<s, d, coord[i]>>}
  /\ net' = net \cup {<<s, (members[s] \ {d}) \cup {n}, coord[i]>>}
  /\ reported' = reported \ {s}
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, members, seen,
                 top, outcome, applies, survive, opSurvive, restarts, regress>>

\* No machine can take the place: the set keeps its size and reports.
Report(i, s) ==
  /\ Writer(i) /\ s \notin reported
  /\ members[s] \cap gone # {} /\ Spares(s) = {}
  /\ reported' = reported \cup {s}
  /\ survive' = IF AutoLower /\ survive > 0 THEN survive - 1 ELSE survive
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, members, seen,
                 top, net, sent, outcome, applies, opSurvive, restarts, regress>>

\* A machine with a new id asks to join; the coordinator records it.
Admit(i, j) ==
  /\ Writer(i) /\ Up(j) /\ j \notin registry /\ j \notin gone
  /\ registry' = registry \cup {j}
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, lead, term, coord, members, seen, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, restarts, regress>>

\* `MachineGone`. The routine writes it once it replaced the id in each set;
\* the model lets it come earlier, as the operator's `OpGone` already takes
\* a gone id out of the registry while a set still names it.
MachineGone(i, d) ==
  /\ Writer(i) /\ d \in registry \cap gone
  /\ registry' = registry \ {d}
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, lead, term, coord, members, seen, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, restarts, regress>>

\* A request of the tenant level: its outcome record and its effect are one
\* write; a later term finds the outcome and does not apply it again.
Apply(i, q) ==
  /\ Writer(i)
  /\ IF Idem THEN \A t \in 1..MaxTerms : <<q, t>> \notin outcome
               ELSE <<q, coord[i]>> \notin outcome
  /\ outcome' = outcome \cup {<<q, coord[i]>>}
  /\ applies' = [applies EXCEPT ![q] = @ + 1]
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, members, seen,
                 top, net, sent, survive, opSurvive, reported, restarts, regress>>

\* The operator lowers the survive number: a request, recorded.
OpLower(i) ==
  /\ Writer(i) /\ opSurvive > 0 /\ opSurvive = 1
  /\ opSurvive' = opSurvive - 1 /\ survive' = opSurvive - 1
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, members, seen,
                 top, net, sent, outcome, applies, reported, restarts, regress>>

\* The operator declares a machine gone before a replacement exists
\* (decision 24), behind the safety check: the live members of each set
\* without it are a quorum. The sets keep their size.
OpGone(i, d) ==
  /\ Writer(i) /\ d \in registry /\ d \notin alive
  /\ GoneCheck => \A s \in Sets : d \in members[s] => Quorum(s, alive \ {d})
  /\ gone' = gone \cup {d} /\ registry' = registry \ {d}
  /\ Wrote(i)
  /\ UNCHANGED <<alive, addr, lead, term, coord, members, seen, top, net, sent,
                 outcome, applies, survive, opSurvive, reported, restarts, regress>>

\* The receiver of a set takes an order, or refuses one below its fence.
Deliver(o) ==
  /\ net' = net \ {o}
  /\ IF OrderFence /\ o[3] < seen[o[1]]
       THEN UNCHANGED <<members, seen, top>>
       ELSE /\ members' = [members EXCEPT ![o[1]] =
                             IF Shrink THEN o[2] \cap alive ELSE o[2]]
            /\ seen' = [seen EXCEPT ![o[1]] = IF o[3] > @ THEN o[3] ELSE @]
            /\ top' = [top EXCEPT ![o[1]] = IF o[3] > @ THEN o[3] ELSE @]
  /\ regress' = (regress \/ (~(OrderFence /\ o[3] < seen[o[1]]) /\ o[3] < top[o[1]]))
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, sent, outcome,
                 applies, survive, opSurvive, reported, restarts, stale>>

-----------------------------------------------------------------------------
\* Faults.

\* A machine is wiped for good, within the kill policy: each set keeps a
\* quorum of machines that are not gone.
Wipe(i) ==
  /\ Cardinality(gone) < Wipes /\ i \in Initial \ gone
  /\ \A s \in Sets : Quorum(s, Initial \ (gone \cup {i}))
  /\ gone' = gone \cup {i} /\ alive' = alive \ {i} /\ addr' = addr \ {i}
  /\ coord' = [coord EXCEPT ![i] = 0]
  /\ UNCHANGED <<registry, lead, term, members, seen, top, net, sent, outcome,
                 applies, survive, opSurvive, reported, restarts, stale, regress>>

\* A new id comes up (Ids \ Initial: NewId, if any): the disk of a wiped
\* machine comes back as a new id (decision 4). The model lets it come up
\* before a wipe too, as a machine that joins the cell, and again after a
\* stop.
Return ==
  /\ alive' = alive \cup (Ids \ Initial) /\ addr' = addr \cup (Ids \ Initial)
  /\ UNCHANGED <<gone, registry, lead, term, coord, members, seen, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, restarts, stale, regress>>

\* A machine stops; its term in memory is lost.
Stop(i) ==
  /\ restarts < MaxRestarts /\ i \in alive \ gone
  /\ restarts' = restarts + 1
  /\ alive' = alive \ {i} /\ coord' = [coord EXCEPT ![i] = 0]
  /\ UNCHANGED <<addr, gone, registry, lead, term, members, seen, top, net, sent,
                 outcome, applies, survive, opSurvive, reported, stale, regress>>

\* It starts again, at a new address.
Start(i) ==
  /\ i \notin alive /\ i \notin gone /\ (i \in Initial \/ i \in registry)
  /\ alive' = alive \cup {i} /\ addr' = addr \ {i}
  /\ UNCHANGED <<gone, registry, lead, term, coord, members, seen, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, restarts, stale, regress>>

\* The new address reaches the cell. The rule: through the journals the
\* machine serves, with no coordinator. The control: the coordinator writes it.
Readdress(i) ==
  /\ i \in alive /\ i \notin addr
  /\ IF Address = "coordinator" THEN \E j \in Ids : Writer(j) /\ Holder(j) ELSE TRUE
  /\ addr' = addr \cup {i}
  /\ UNCHANGED <<alive, gone, registry, lead, term, coord, members, seen, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, restarts, stale, regress>>

\* A receiver of orders restarts.
RestartReceiver(s) ==
  /\ restarts < MaxRestarts /\ ~Durable /\ seen[s] > 0
  /\ restarts' = restarts + 1
  /\ seen' = [seen EXCEPT ![s] = 0]
  /\ UNCHANGED <<alive, addr, gone, registry, lead, term, coord, members, top, net,
                 sent, outcome, applies, survive, opSurvive, reported, stale, regress>>

Duty ==
  \/ \E i \in Ids : Campaign(i)
  \/ \E i \in Ids, s \in Sets, d, n \in Ids : Replace(i, s, d, n)
  \/ \E i \in Ids, s \in Sets : Report(i, s)
  \/ \E i, j \in Ids : Admit(i, j) \/ MachineGone(i, j)
  \/ \E i \in Ids, q \in Requests : Apply(i, q)
  \/ \E o \in net : Deliver(o)
  \/ \E i \in Ids : Start(i) \/ Readdress(i)
  \/ Return

Next ==
  \/ Duty
  \/ \E i \in Ids : OpLower(i) \/ Wipe(i) \/ Stop(i)
  \/ \E i, d \in Ids : OpGone(i, d)
  \/ \E s \in Sets : RestartReceiver(s)

Spec == Init /\ [][Next]_vars

\* Liveness: faults stop, and every duty that stays enabled is taken.
LiveSpec ==
  /\ Spec
  /\ \A i \in Ids : WF_vars(Campaign(i)) /\ WF_vars(Start(i)) /\ WF_vars(Readdress(i))
  /\ \A i \in Ids, s \in Sets, d, n \in Ids : WF_vars(Replace(i, s, d, n))
  /\ \A i \in Ids, s \in Sets : WF_vars(Report(i, s))
  /\ \A i, j \in Ids : WF_vars(Admit(i, j)) /\ WF_vars(MachineGone(i, j))
  /\ WF_vars(\E o \in net : Deliver(o))
  /\ WF_vars(Return)

-----------------------------------------------------------------------------
\* Safety.
OneWriter == ~stale
NoRegress == ~regress
SizeKept == \A s \in Sets : Cardinality(members[s]) = Size(s)
ExactlyOnce == \A q \in Requests : applies[q] <= 1
SurviveKept == survive = opSurvive
\* No set counts on fewer than a quorum of ids that are not gone.
QuorumKept == \A s \in Sets : Quorum(s, (Initial \cup registry) \ gone)

\* Liveness.
Full(s) == \A i \in members[s] : Up(i) /\ i \notin gone
Healed == \A s \in Sets : Full(s) \/ (s \in reported /\ Spares(s) = {})
ServingNow == Coordinator /\ Serves("cj")
CoordinatorServes == []<>ServingNow
Heals == <>[]Healed
NoDeadId == <>[](registry \cap gone = {})
=============================================================================

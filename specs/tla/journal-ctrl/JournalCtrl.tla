----------------------------- MODULE JournalCtrl -----------------------------
(***************************************************************************)
(* CTRL in the journal roles, over `JournalCompact.tla` (section J5.3,     *)
(* #1263 (CTRL recovery in the journal), #1326 (the journal roles over the *)
(* core)). `ctrl-grid/` checks each rule of CTRL alone; this module puts   *)
(* the rules into the composed model, so that TLC checks their seams with  *)
(* Paxos in each segment, the seal, the compaction, retire and trim:       *)
(*                                                                         *)
(*   - A boot reads a damaged vote as a faulty vote: the ballot stays and  *)
(*     the value is lost (`Rot`). `Journal.tla` blocks a slot whose top    *)
(*     report is a faulty vote, and the seal end counts it (#1313).        *)
(*   - The leader's probe decides a blocked slot on a promise quorum of    *)
(*     each grid that reads it whose answers all qualify: no faulty vote   *)
(*     above the highest readable one (`ProbeDecide`, the tri-state tally  *)
(*     of `slot_decidable`). It proposes the highest readable vote, or a   *)
(*     `Noop` when there is none.                                          *)
(*   - The leader learns the chosen value of a blocked slot from a replica *)
(*     that applied it, and proposes it (`LearnChosen`).                   *)
(*   - An acceptor repairs a faulty vote in one write: the value that a    *)
(*     replica applied, under the ballot that the faulty vote keeps        *)
(*     (`AccRepair`).                                                      *)
(*                                                                         *)
(* Unchoose "any" is a control: a vote counts as unchoosable without the   *)
(* promises above its ballot, a later `Accept` can still complete it, and  *)
(* `BlockedChosen` breaks (`ctrl_unchoose`).                               *)
(*                                                                         *)
(* Damage "keep" and Repair "chosen" are the design. Damage "drop" is a    *)
(* control: a boot that drops the damaged vote reads the slot as a slot    *)
(* with no vote, and `Agreement` breaks. Repair "peer" is a control: the   *)
(* value comes from another acceptor's vote that is not known to be        *)
(* chosen, and `OneValueAtBallot` breaks.                                  *)
(***************************************************************************)
EXTENDS JournalCompact

CONSTANTS MaxRot, Damage, Repair, ProbeDrops, Unchoose

ASSUME MaxRot \in Nat
ASSUME Damage \in {"keep", "drop"}
ASSUME Repair \in {"chosen", "peer"}
ASSUME ProbeDrops \in {"skip", "ignore"}
ASSUME Unchoose \in {"promised", "any"}

Faulty(x) == x[1] > 0 /\ x[2] = None

\* A replica applied slot s: the slot is chosen, and the replica knows its
\* value.
Learned(s) == \E r \in Replicas : Holds(r, s)

\* a's vote at s is at a ballot f that no quorum can choose at: a promise
\* quorum of f's grid, without a, holds no vote at s, on disk or in memory,
\* sent no vote at s and f, kept s, and promised above f on disk. Every
\* quorum of f's grid meets it, so no quorum votes at f. A later leader's
\* tally may read a's damaged vote with that quorum, find no readable vote,
\* and decide a `Noop` (`ProbeDecide`).
Unchoosable(a, s) ==
  LET f == dvote[a][s][1]
  IN
  /\ f > 0
  /\ \E Q \in QOf[bgen[f]] :
       /\ a \notin Q
       /\ \A x \in Q :
            /\ dvote[x][s] = NoVote
            /\ mvote[x][s] = NoVote
            /\ s \notin adrop[x] \cup dadrop[x]
            /\ Unchoose = "any" \/ dprom[x] > f
            /\ \A v \in Cmds : <<s, f, v, x>> \notin sent2b

\* The damage of a's vote at s leaves the value readable for good: a
\* replica applied the slot, the slot chose another value, or a quorum of a
\* grid that still runs holds the chosen value on disk without a, each copy
\* at or above the ballot that chose it (a later `Accept` there carries the
\* same value, P2c; a copy below it can still change). A damaged copy is one
\* of the F failures that a grid survives; a rot of a copy that no lasting
\* source holds is data loss, beyond the contract (`Readable`). Or no
\* ballot can choose the value of a's vote (`Unchoosable`): the value is not
\* needed, and the slot may stay unchosen.
Spare(a, s) ==
  \/ Unchoosable(a, s)
  \/ Learned(s)
  \/ IsChosen(s) /\ dvote[a][s][2] # ChosenVal(s)
  \/ /\ IsChosen(s)
     /\ \E g \in Live : \E Q \in Q2Of(g, s) :
          /\ a \notin Q
          /\ \A x \in Q :
               /\ dvote[x][s][1] > 0
               /\ bgen[dvote[x][s][1]] = g
               /\ dvote[x][s][2] = ChosenVal(s)
               /\ \E c \in Ballots : c <= dvote[x][s][1] /\ ChosenAtBal(s, ChosenVal(s), c)

\* Acceptor a boots and reads its vote at s damaged. The boot is a crash:
\* the queue and the memory are lost, and the role starts from its disk. A
\* slot with no vote, or a faulty one, reads the same after the damage.
Rot(a, s) ==
  LET d == IF Damage = "keep" THEN <<dvote[a][s][1], None>> ELSE NoVote
  IN
  /\ rots < MaxRot
  /\ Spare(a, s)
  /\ rots' = rots + 1
  /\ dvote' = [dvote EXCEPT ![a][s] = d]
  /\ mvote' = [mvote EXCEPT ![a] = [dvote[a] EXCEPT ![s] = d]]
  /\ mprom' = [mprom EXCEPT ![a] = dprom[a]]
  /\ adrop' = [adrop EXCEPT ![a] = dadrop[a]]
  /\ pend' = [pend EXCEPT ![a] = <<>>]
  /\ UNCHANGED <<reg, ldr, dprom, crashes, p1b, sent2b, dadrop, rep>>

\* The acceptors of Q answered the probe of b. With `Direct` the probe asks
\* them in the step (an acceptor that promised b already answers again);
\* else it takes the promises they sent.
ProbePromised(b, Q) ==
  IF Direct THEN \A a \in Q : a \in Active /\ mprom[a] <= b
  ELSE Promised(b, Q)

\* The probe of b decides the blocked slot s on the promise quorum Qs[h] of
\* each configuration h that reads s: every answer qualifies, so a faulty
\* vote is at or below the highest readable vote. No answer dropped s: an
\* answer that dropped it reports no vote there, and the other answers may
\* predate the compaction, so the tally would read a stale vote
\* (`ProbeSkip`). The control "ignore" tallies the slot anyway, and
\* `Agreement` breaks. With no readable vote the decision is a `Noop`, which
\* the leader proposes at s (`JournalLeader`), whatever slots it saw above.
ProbeDecide(b, s, Qs) ==
  LET hs == {h \in bH[b] : s \in ReadSet(b, h)}
      rs == UNION {Reports(b, Qs[h]) : h \in hs}
      xs == {x[2][s] : x \in rs}
      rd == {x \in xs : x[1] > 0 /\ x[2] # None}
      thr == Max({0} \cup {x[1] : x \in rd})
      all == UNION {Qs[h] : h \in hs}
  IN
  /\ s \in bblock[b]
  /\ \A h \in hs : Qs[h] \in QOf[h] /\ ProbePromised(b, Qs[h])
  /\ ProbeDrops = "ignore" \/ \A x \in rs : s \notin x[3]
  /\ \A x \in xs : ~(Faulty(x) /\ x[1] > thr)
  /\ bsafe' = [bsafe EXCEPT ![b][s] =
       IF rd = {} THEN Noop ELSE (CHOOSE x \in rd : x[1] = thr)[2]]
  /\ bblock' = [bblock EXCEPT ![b] = @ \ {s}]
  /\ IF Direct
     THEN /\ mprom' = [a \in Acceptors |-> IF a \in all THEN b ELSE mprom[a]]
          /\ dprom' = [a \in Acceptors |-> IF a \in all THEN b ELSE dprom[a]]
     ELSE UNCHANGED <<mprom, dprom>>
  /\ UNCHANGED <<reg, bst, bH, bE, bend, blo, bstart, prop, bskip, mvote, pend, dvote,
                 crashes, rots, p1b, sent2b, adrop, dadrop, rep>>

\* The answer of a, a late one too, dropped the blocked slot s of b: s is
\* chosen and applied at F + 1 replicas (`JournalCompact.tla`), so the
\* leader proposes nothing there, as Phase 1 skips a dropped slot
\* (`SkipCompacted`). Any report that dropped s proves it, whatever ballot
\* it answers: an acceptor drops a slot only after the compaction.
ProbeSkip(b, s, a) ==
  /\ s \in bblock[b]
  /\ \E x \in Reports(b, {a}) : s \in x[3]
  /\ bblock' = [bblock EXCEPT ![b] = @ \ {s}]
  /\ bskip' = [bskip EXCEPT ![b] = @ \cup {s}]
  /\ UNCHANGED <<reg, bst, bH, bE, bsafe, bend, blo, bstart, prop, acc, rep>>

\* The leader of b learns the chosen value of its blocked slot s from a
\* replica that applied it, and proposes it. The answer may arrive after the
\* replica dropped the slot. A replica applies only a chosen slot, so s is
\* chosen; a slot that a rot of an `Unchoosable` vote blocked may be not.
LearnChosen(b, s) ==
  /\ s \in bblock[b]
  /\ IsChosen(s)
  /\ bsafe' = [bsafe EXCEPT ![b][s] = ChosenVal(s)]
  /\ bblock' = [bblock EXCEPT ![b] = @ \ {s}]
  /\ UNCHANGED <<reg, bst, bH, bE, bend, blo, bstart, prop, bskip, acc, rep>>

\* Acceptor a repairs its faulty vote at s in one write. The design
\* (`paros_consensus::Acceptor::learn_chosen`) takes a value v chosen at a
\* ballot c and writes it at the higher of c and the ballot that the faulty
\* vote keeps: a vote above c carries the chosen value (P2c), and a vote
\* below c gives way to the vote at c. The leader sends it once its own
\* round at c chose v. The promise rises to that ballot in the same write.
\* The control "peer" takes another acceptor's vote under the faulty ballot.
\* Only a faulty vote takes the repair: a repair of a readable vote would
\* raise the promise with no `Prepare`, and `PromiseRead` breaks.
AccRepair(a, s, v, c) ==
  LET f == mvote[a][s][1]
      at == IF Repair = "chosen" THEN Max({f, c}) ELSE f
      w == <<at, v>>
      raise == mprom[a] < at
  IN
  \* The acceptors of a retired configuration stop (`PromiseRead`).
  /\ a \in Active
  \* A dropped slot holds no vote (`DroppedEmpty`), so it is not faulty.
  /\ Faulty(mvote[a][s])
  /\ IF Repair = "chosen"
     THEN ChosenAtBal(s, v, c)
     ELSE \E p \in Acceptors \ {a} : mvote[p][s][1] > 0 /\ mvote[p][s][2] = v
  /\ mvote' = [mvote EXCEPT ![a][s] = w]
  /\ mprom' = [mprom EXCEPT ![a] = IF raise THEN at ELSE @]
  /\ IF Direct
     THEN /\ dvote' = [dvote EXCEPT ![a][s] = w]
          /\ dprom' = [dprom EXCEPT ![a] = IF raise THEN at ELSE @]
          /\ UNCHANGED pend
     ELSE /\ pend' = [pend EXCEPT ![a] =
               Append(@, <<IF raise THEN "pv" ELSE "v", s, at, v, FALSE>>)]
          /\ UNCHANGED <<dvote, dprom>>
  /\ UNCHANGED <<reg, ldr, crashes, rots, p1b, sent2b, adrop, dadrop, rep>>

NextR ==
  \/ NextC
  \/ \E a \in Acceptors, s \in Slots : Rot(a, s)
  \/ \E b \in Ballots, s \in Slots : LearnChosen(b, s)
  \/ \E b \in Ballots, s \in Slots :
       \E Qs \in [bH[b] -> UNION {QOf[h] : h \in Gens}] : ProbeDecide(b, s, Qs)
  \/ \E b \in Ballots, s \in Slots, a \in Acceptors : ProbeSkip(b, s, a)
  \/ \E a \in Acceptors, s \in Slots, v \in Cmds, c \in Ballots : AccRepair(a, s, v, c)

SpecR == Init /\ [][NextR]_vars

\* CTRL with an answer to a Prepare sent again (`Repromise`): a second
\* answer at a ballot that the probe, the skip and the decide read too.
NextRR == NextR \/ \E a \in Acceptors, b \in Ballots : Repromise(a, b)

SpecRR == Init /\ [][NextRR]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Two readable votes at one slot and one ballot carry one value (P2c),
\* in memory and on disk.
OneValueAtBallot == \A s \in Slots, a, c \in Acceptors :
  \A x, y \in {mvote[a][s], dvote[a][s], mvote[c][s], dvote[c][s]} :
    x[1] > 0 /\ y[1] = x[1] /\ x[2] # None /\ y[2] # None => x[2] = y[2]

\* A leader never proposes at a slot that its Phase 1 left blocked.
\* A slot that a leader skips is chosen and applied at CompactAt replicas
\* of its segment.
SkipCompacted == \A b \in Ballots : \A s \in bskip[b] :
  \E p \in Gens : StartKnown(p) /\ Start(p) <= s /\ CompactedTo(p, s + 1)
NoProposeBlocked == \A b \in Ballots : \A s \in bblock[b] : prop[b][s] = None

\* No quorum of the grid of ballot f can vote at s at f: a promise quorum of
\* it, without a, promised above f on disk and sent no vote at s and f.
\* Both facts last: a promise only rises, and a sent vote stays sent.
DeadAt(a, s, f) ==
  /\ bgen[f] # 0
  /\ \E Q \in QOf[bgen[f]] :
       /\ a \notin Q
       /\ \A x \in Q : dprom[x] > f /\ \A v \in Cmds : <<s, f, v, x>> \notin sent2b

\* A blocked slot is chosen, or the highest ballot that voted there is a
\* ballot that no quorum can complete: a rot needs a lasting copy of a
\* chosen value, or a vote that no quorum can choose (`Spare`), and the
\* blocked slot's top report is a damaged vote.
BlockedChosen == \A b \in Ballots : \A s \in bblock[b] :
  \/ IsChosen(s)
  \/ \E a \in Acceptors, f \in Ballots, v \in Cmds :
       /\ <<s, f, v, a>> \in sent2b
       /\ \A m \in sent2b : m[1] = s => m[2] <= f
       /\ DeadAt(a, s, f)

(***************************************************************************)
(* Reach: each must be violated, so that a passing check is not empty.     *)
(***************************************************************************)
NoBlocked == \A b \in Ballots : bblock[b] = {}
\* No blocked slot is skipped.
NoSkip == \A b \in Ballots : bskip[b] = {}
\* No blocked slot opens. With one acceptor in a grid the probe never
\* decides (the faulty vote is the only answer), so this reaches `LearnChosen`.
NoUnblock == [][\A b \in Ballots : bblock[b] \subseteq bblock'[b]]_vars
\* No blocked slot opens by a step of the probe: the step names the action,
\* not the state.
NoProbe == [][\A b \in Ballots, s \in Slots :
               s \in bblock[b] /\ s \notin bblock'[b] =>
                 ~\E Qs \in [bH[b] -> UNION {QOf[h] : h \in Gens}] : ProbeDecide(b, s, Qs)]_vars
\* No blocked slot that is not chosen opens with a `Noop`. Only a rot of an
\* `Unchoosable` vote blocks a slot that is not chosen (`BlockedChosen`), so
\* this reaches the probe that decides a `Noop` after such a rot.
NoNoopProbe == [][\A b \in Ballots, s \in Slots :
                  s \in bblock[b] /\ s \notin bblock'[b] /\ bsafe'[b][s] = Noop =>
                    IsChosen(s)]_vars
NoRepair == [][\A a \in Acceptors, s \in Slots :
                Faulty(mvote[a][s]) => ~(mvote'[a][s][1] = mvote[a][s][1] /\ mvote'[a][s][2] # None)]_vars
=============================================================================

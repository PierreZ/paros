----------------------------- MODULE MultiWriter -----------------------------
(***************************************************************************)
(* Multi-writer mode through SetLeader(None) and expected_seq, #1274       *)
(* (multi-writer mode through SetLeader(None) and expected_seq).           *)
(*                                                                         *)
(* The decisions it checks (Pierre, 2026-11-20):                           *)
(*   - `SetLeader(new, old)` may name `new = None`. The journal is then    *)
(*     multi-writer. The mode is what the last applied `SetLeader` left.   *)
(*   - There is no dedup table. A single writer gets exactly-once through  *)
(*     `expected_seq`; multi-writer is at-least-once.                      *)
(*   - A held write is answered `Unknown` (#1232 (held writes)). The       *)
(*     client resolves it by a resend with the same `expected_seq`.        *)
(*   - At a seal, the leader uuid of the boundary state goes into the      *)
(*     journal's matchmaker record, and the first leader of the next       *)
(*     segment proposes it as a `Fence(uuid)` at the first slot of the     *)
(*     segment (#1271 (a write reaches only the replica set of its         *)
(*     segment)). Here the uuid can be None.                               *)
(*                                                                         *)
(* The log is abstract: the slots are chosen in order (`multi-paxos/`      *)
(* checks the agreement). A command is proposed into the next free slot.   *)
(* A request stays in the network after it is proposed, so it can be       *)
(* proposed again: that is a resend of the client, a copy that the         *)
(* transport made, or a write that an old consensus leader proposed in     *)
(* another slot. One fold judges each slot at apply, in slot order, and    *)
(* records one answer. A client may never see an answer: that is the held  *)
(* write that is answered `Unknown`. The invariants judge every answer,    *)
(* seen or not, so the answer to a resend is judged too.                   *)
(*                                                                         *)
(* A fenced write is [u, e, op]: the leader uuid, `expected_seq` and the   *)
(* write. An unfenced write is [op]. Each uuid is fresh: the k-th          *)
(* `SetLeader` names [w, k] (a reinstated uuid is decision C of section    *)
(* 2.3 and is out of this model).                                          *)
(*                                                                         *)
(* The fold of a fenced write, in this order (the rule):                   *)
(*   1. e < first_seq: "truncated" (the write's fate is lost).             *)
(*   2. e < next_seq: "dup" with the seq when the record at e is this     *)
(*      write (same uuid, same op), else "taken". After a seal, a record  *)
(*      of the old segment is not in the fold: see Lookup.                 *)
(*   3. the leader is not u (another uuid, None or unset): "fenced".       *)
(*   4. e > next_seq: "early" (a write in front of it is not applied yet). *)
(*   5. else "applied" at e.                                               *)
(* An unfenced write is applied when the leader is None, else "wrongmode". *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control.                                               *)
(*   FenceAt   "apply": the uuid of a write or a truncate is judged at     *)
(*             apply. "propose": it is judged against the applied state    *)
(*             when the command is proposed, and not again at apply.       *)
(*   Dedup     "first": steps 1 and 2 come before step 3. "after": the     *)
(*             fence comes first, so the resend of a write that was        *)
(*             applied before a switch is answered "fenced".               *)
(*   Basis     "view": a writer whose SetLeader applied takes its next     *)
(*             `expected_seq` from the view of the answer. "stale": it     *)
(*             keeps the basis it had.                                     *)
(*   Boundary  "wait": the first leader of the next segment reads the uuid *)
(*             in the matchmaker record only after the seal wrote it.      *)
(*             "early": it may read it before.                             *)
(*   Multi     "tagged": `SetLeader(None, old)` names a fresh multi tag,   *)
(*             and the journal leads with that tag, so a late copy of a    *)
(*             `SetLeader(new, old)` whose `old` was the multi-writer mode *)
(*             does not match a later multi-writer mode (#1495). "plain":  *)
(*             a control, every multi-writer mode is the same `None`, so   *)
(*             such a copy brings its uuid back and a refused fenced write *)
(*             lands.                                                      *)
(*   InitLeader None or Unset: the leader of a fresh journal. Unset        *)
(*             refuses every write and truncate until a SetLeader.         *)
(*                                                                         *)
(*   Lookup    what the fold of the next segment answers for a fenced      *)
(*             write whose position is in the old segment, below the       *)
(*             boundary (#1301 (a resent write across a seal)). The fold   *)
(*             of a segment holds the boundary state, not the old records. *)
(*             "boundary": "below", not final; the client reads that       *)
(*             position from the old segment's replica set (the rule).     *)
(*             "global": the fold reads the old record (a read across      *)
(*             segments at apply). "local": a control, the fold finds no   *)
(*             record and answers "taken" (`apply_write` as it is).        *)
(*             "digest": a control, the boundary state keeps the last      *)
(*             DigestSize records of the old segment, older is "taken".    *)
(*                                                                         *)
(* A client that sees a final refusal ("fenced" or "taken") of a fenced    *)
(* write while the view says None sends the write again, unfenced: it now  *)
(* writes in multi-writer mode.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
  Writers,     \* the clients
  None,        \* the leader of a multi-writer journal
  Unset,       \* the leader of a fresh journal, with InitLeader = Unset
  NoLead,      \* a client that never led
  MaxOps,      \* writes of each client
  MaxSL,       \* SetLeader commands in a run
  MaxSlots,    \* slots of the log
  SealAt,      \* the first slot of the next segment, 0 for no seal
  InitLeader,
  FenceAt, Dedup, Basis, Boundary, Lookup, Multi,
  DigestSize   \* the old records that the "digest" control keeps

ASSUME FenceAt \in {"apply", "propose"}
ASSUME Dedup \in {"first", "after"}
ASSUME Basis \in {"view", "stale"}
ASSUME Boundary \in {"wait", "early"}
ASSUME InitLeader \in {None, Unset}
ASSUME Lookup \in {"boundary", "global", "local", "digest"}
ASSUME Multi \in {"tagged", "plain"}
ASSUME DigestSize \in Nat
ASSUME SealAt = 0 \/ (SealAt >= 2 /\ SealAt < MaxSlots)

Uuids == [w : Writers, k : 1..MaxSL]
\* The multi tags: the k-th `SetLeader` that names None leads with [m |-> k].
Tags == [m : 1..MaxSL]
Leaders == Uuids \cup Tags \cup {None, Unset}

\* The journal is multi-writer: a fresh journal with InitLeader = None, or a
\* multi tag.
IsMulti(l) == l = None \/ l \in Tags
Ops == [w : Writers, n : 1..MaxOps]

VARIABLES
  slots,      \* the chosen log, Seq of commands
  applied,    \* slots applied by the fold
  leader,     \* fold: the leader uuid, None or Unset
  first,      \* fold: first_seq
  log,        \* fold: the records, log[s] is the record at seq s
  truncs,     \* fold: the accepted truncates, [u, slot]
  answers,    \* every answer of the fold
  sent,       \* the requests in the network
  lead,       \* client: the uuid that it last won, or NoLead
  basis,      \* client: its next expected_seq
  nops,       \* client: writes sent so far
  slCount,
  meta,       \* the leader uuid in the journal's matchmaker record
  metaSet,    \* the seal wrote it
  starts      \* the next_seq at which each uuid started to lead

vars == <<slots, applied, leader, first, log, truncs, answers, sent, lead,
          basis, nops, slCount, meta, metaSet, starts>>

NextSeq == Len(log) + 1

----------------------------------------------------------------------------
(* The commands.                                                           *)

Fenced(u, e, op) == [kind |-> "f", u |-> u, e |-> e, op |-> op]
Unfenced(op) == [kind |-> "u", op |-> op]

\* `ok` is the propose-time check: used only with FenceAt = "propose".
WithCheck(m, ok) == m @@ [ok |-> ok]

ProposeOk(m) ==
  CASE m.kind = "f" -> leader = m.u
    [] m.kind = "u" -> IsMulti(leader)
    [] m.kind = "t" -> \/ IsMulti(leader) /\ m.u = None
                       \/ leader \in Uuids /\ m.u = leader
    [] OTHER -> TRUE

\* The slot that a new command takes is free and is not the Fence slot.
CanAppend == Len(slots) < MaxSlots /\ Len(slots) + 1 # SealAt

AppendCmd(cmd) == slots' = Append(slots, WithCheck(cmd, ProposeOk(cmd)))

----------------------------------------------------------------------------
(* The fold, at apply.                                                     *)

FenceOk(c) == IF FenceAt = "apply" THEN leader = c.u ELSE c.ok

TruncOk(c) ==
  IF FenceAt = "apply"
  THEN \/ IsMulti(leader) /\ c.u = None
       \/ leader \in Uuids /\ c.u = leader
  ELSE c.ok

\* The first seq of the next segment: next_seq of the boundary state.
BoundarySeq == 1 + Cardinality({k \in DOMAIN log : log[k].slot < SealAt})

\* Slot s is in the next segment, and the record at position e is in the old.
OldRecord(e, s) == SealAt > 0 /\ s > SealAt /\ log[e].slot < SealAt

\* The answer from the record at position e.
Match(c) == IF log[c.e].u = c.u /\ log[c.e].op = c.op THEN "dup" ELSE "taken"

\* The answer for a position below next_seq, judged at slot s.
Below(c, s) ==
  IF ~OldRecord(c.e, s) THEN Match(c)
  ELSE CASE Lookup = "boundary" -> "below"
         [] Lookup = "global" -> Match(c)
         [] Lookup = "local" -> "taken"
         [] Lookup = "digest" ->
              IF c.e >= BoundarySeq - DigestSize THEN Match(c) ELSE "taken"

Position(c, s) ==
  IF c.e < first THEN "truncated"
  ELSE IF c.e < NextSeq THEN Below(c, s)
       ELSE IF c.e > NextSeq THEN "early" ELSE "applied"

JudgeFenced(c, s) ==
  IF Dedup = "first"
  THEN IF c.e < NextSeq THEN Position(c, s)
       ELSE IF ~FenceOk(c) THEN "fenced" ELSE Position(c, s)
  ELSE IF ~FenceOk(c) THEN "fenced" ELSE Position(c, s)

Answer(c, kind, s, slot) ==
  [m |-> [k \in {"kind", "u", "e", "op"} \cap DOMAIN c |-> c[k]],
   kind |-> kind, seq |-> s, leader |-> leader, slot |-> slot]

\* The seal: the fold of the old segment writes the boundary uuid after its
\* last slot.
Seal(s, newLeader) ==
  IF SealAt > 0 /\ s = SealAt - 1
  THEN meta' = newLeader /\ metaSet' = TRUE
  ELSE UNCHANGED <<meta, metaSet>>

ApplyFenced(c, s) ==
  LET a == JudgeFenced(c, s) IN
  /\ IF a = "applied"
     THEN log' = Append(log, [u |-> c.u, op |-> c.op, slot |-> s])
     ELSE UNCHANGED log
  /\ answers' = answers \cup
       {Answer(c, a, IF a = "dup" THEN c.e ELSE IF a = "applied" THEN NextSeq ELSE 0, s)}
  /\ UNCHANGED <<leader, first, truncs, lead, basis, starts>>
  /\ Seal(s, leader)

ApplyUnfenced(c, s) ==
  LET a == IF IsMulti(leader) THEN "applied" ELSE "wrongmode" IN
  /\ IF a = "applied"
     THEN log' = Append(log, [u |-> None, op |-> c.op, slot |-> s])
     ELSE UNCHANGED log
  /\ answers' = answers \cup {Answer(c, a, IF a = "applied" THEN NextSeq ELSE 0, s)}
  /\ UNCHANGED <<leader, first, truncs, lead, basis, starts>>
  /\ Seal(s, leader)

\* SetLeader is a compare-and-set. A writer that won takes its basis from the
\* view of the answer: next_seq after the SetLeader.
ApplySetLeader(c, s) ==
  IF leader = c.old /\ c.new # leader
  THEN /\ leader' = c.new
       /\ IF c.new \in Uuids
          THEN /\ lead' = [lead EXCEPT ![c.new.w] = c.new]
               /\ basis' = [basis EXCEPT ![c.new.w] =
                              IF Basis = "view" THEN NextSeq ELSE @]
               /\ starts' = starts \cup {[u |-> c.new, s |-> NextSeq, from |-> c.old]}
          ELSE UNCHANGED <<lead, basis, starts>>
       /\ UNCHANGED <<first, log, truncs, answers>>
       /\ Seal(s, c.new)
  ELSE /\ UNCHANGED <<leader, first, log, truncs, answers, lead, basis, starts>>
       /\ Seal(s, leader)

ApplyTruncate(c, s) ==
  /\ IF TruncOk(c)
     THEN /\ first' = IF c.upto > first
                      THEN IF c.upto < NextSeq THEN c.upto ELSE NextSeq
                      ELSE first
          /\ truncs' = truncs \cup {[u |-> c.u, slot |-> s]}
     ELSE UNCHANGED <<first, truncs>>
  /\ UNCHANGED <<leader, log, answers, lead, basis, starts>>
  /\ Seal(s, leader)

\* The first slot of the next segment: it sets the leader of the new fold.
ApplyFence(c, s) ==
  /\ leader' = c.leader
  /\ UNCHANGED <<first, log, truncs, answers, lead, basis, starts, meta, metaSet>>

Apply ==
  /\ applied < Len(slots)
  /\ LET s == applied + 1
         c == slots[s]
     IN CASE c.kind = "f" -> ApplyFenced(c, s)
          [] c.kind = "u" -> ApplyUnfenced(c, s)
          [] c.kind = "sl" -> ApplySetLeader(c, s)
          [] c.kind = "t" -> ApplyTruncate(c, s)
          [] c.kind = "fence" -> ApplyFence(c, s)
  /\ applied' = applied + 1
  /\ UNCHANGED <<slots, sent, nops, slCount>>

----------------------------------------------------------------------------
(* The clients and the leader that proposes.                               *)

\* A client calls SetLeader(new, old): `old` is the leader it read, `new` is a
\* fresh uuid of its own, or the multi-writer mode: a fresh tag (Multi =
\* "tagged") or None. The call races with the slots in flight. The request
\* stays in the network, so a copy of it can be proposed again later.
CallSetLeader(w, toNone) ==
  /\ slCount < MaxSL
  /\ CanAppend
  /\ LET new == IF ~toNone THEN [w |-> w, k |-> slCount + 1]
                ELSE IF Multi = "tagged" THEN [m |-> slCount + 1] ELSE None
         cmd == [kind |-> "sl", new |-> new, old |-> leader]
     IN /\ AppendCmd(cmd)
        /\ sent' = sent \cup {cmd}
  /\ slCount' = slCount + 1
  /\ UNCHANGED <<applied, leader, first, log, truncs, answers, lead,
                 basis, nops, meta, metaSet, starts>>

\* A client that won a uuid writes under it, pipelined: each write at the
\* position after the last one. A deposed client keeps writing.
SendFenced(w) ==
  /\ lead[w] # NoLead
  /\ nops[w] < MaxOps
  /\ sent' = sent \cup {Fenced(lead[w], basis[w], [w |-> w, n |-> nops[w] + 1])}
  /\ basis' = [basis EXCEPT ![w] = @ + 1]
  /\ nops' = [nops EXCEPT ![w] = @ + 1]
  /\ UNCHANGED <<slots, applied, leader, first, log, truncs, answers, lead,
                 slCount, meta, metaSet, starts>>

\* Any client may send an unfenced write, in any mode.
SendUnfenced(w) ==
  /\ nops[w] < MaxOps
  /\ sent' = sent \cup {Unfenced([w |-> w, n |-> nops[w] + 1])}
  /\ nops' = [nops EXCEPT ![w] = @ + 1]
  /\ UNCHANGED <<slots, applied, leader, first, log, truncs, answers, lead,
                 basis, slCount, meta, metaSet, starts>>

\* A client whose fenced write got a final refusal while the journal is
\* multi-writer writes it again, unfenced (Next takes a in answers). The
\* model lets a client do so after any final refusal, in any mode: an
\* unfenced write is allowed in any mode (SendUnfenced), and the resend of
\* an unfenced write is the same message.
MoveToMulti(a) ==
  /\ a.kind \in {"fenced", "taken"}
  /\ sent' = sent \cup {Unfenced(a.m.op)}
  /\ UNCHANGED <<slots, applied, leader, first, log, truncs, answers, lead,
                 basis, nops, slCount, meta, metaSet, starts>>

\* A client that got "below" reads that position from the replica set of
\* the old segment (a sealed segment does not change) and learns the fate
\* of its write: "dup" when the record there is its write, "truncated"
\* when the position is below first_seq, else "taken".
ResolveBelow(a) ==
  /\ a.kind = "below"
  /\ LET k == IF a.m.e < first THEN "truncated"
              ELSE IF log[a.m.e].u = a.m.u /\ log[a.m.e].op = a.m.op THEN "dup"
              ELSE "taken"
     IN answers' = answers \cup
          {[m |-> a.m, kind |-> k, seq |-> IF k = "dup" THEN a.m.e ELSE 0,
            leader |-> leader, slot |-> 0]}
  /\ UNCHANGED <<slots, applied, leader, first, log, truncs, sent, lead,
                 basis, nops, slCount, meta, metaSet, starts>>

\* A truncate up to the tail that the client read: fenced by the uuid that it
\* won, or unfenced. A client that never won a uuid sends NoLead, which no
\* apply takes. MaxSlots bounds the truncates.
CallTruncate(w, fenced) ==
  /\ CanAppend
  /\ AppendCmd([kind |-> "t", u |-> IF fenced THEN lead[w] ELSE None, upto |-> NextSeq])
  /\ UNCHANGED <<applied, leader, first, log, truncs, answers, sent, lead,
                 basis, nops, slCount, meta, metaSet, starts>>

\* The consensus leader proposes a request from the network: a first send,
\* a resend or a copy.
Propose(m) ==
  /\ CanAppend
  /\ AppendCmd(m)
  /\ UNCHANGED <<applied, leader, first, log, truncs, answers, sent, lead,
                 basis, nops, slCount, meta, metaSet, starts>>

\* The first leader of the next segment proposes the Fence with the uuid in
\* the matchmaker record.
ProposeFence ==
  /\ Len(slots) + 1 = SealAt
  /\ Boundary = "wait" => metaSet
  /\ slots' = Append(slots, [kind |-> "fence", leader |-> meta, ok |-> TRUE])
  /\ UNCHANGED <<applied, leader, first, log, truncs, answers, sent, lead,
                 basis, nops, slCount, meta, metaSet, starts>>

----------------------------------------------------------------------------

Init ==
  /\ slots = <<>>
  /\ applied = 0
  /\ leader = InitLeader
  /\ first = 1
  /\ log = <<>>
  /\ truncs = {}
  /\ answers = {}
  /\ sent = {}
  /\ lead = [w \in Writers |-> NoLead]
  /\ basis = [w \in Writers |-> 1]
  /\ nops = [w \in Writers |-> 0]
  /\ slCount = 0
  /\ meta = InitLeader
  /\ metaSet = FALSE
  /\ starts = {}

Next ==
  \/ Apply
  \/ \E w \in Writers : \/ CallSetLeader(w, TRUE) \/ CallSetLeader(w, FALSE)
                         \/ SendFenced(w) \/ SendUnfenced(w)
                         \/ CallTruncate(w, TRUE) \/ CallTruncate(w, FALSE)
  \/ \E a \in answers : MoveToMulti(a) \/ ResolveBelow(a)
  \/ \E m \in sent : Propose(m)
  \/ ProposeFence

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* The oracle: the leader at each slot from the SetLeader commands alone,  *)
(* by their compare-and-set, with no segment and no fold.                  *)

RECURSIVE OracleUpTo(_)
OracleUpTo(s) ==
  IF s = 0 THEN InitLeader
  ELSE LET l == OracleUpTo(s - 1)
           c == slots[s]
       IN IF c.kind = "sl" /\ c.old = l /\ c.new # l THEN c.new ELSE l

\* The leader that judges slot s: the one that the slots before it left.
OracleAt(s) == OracleUpTo(s - 1)

----------------------------------------------------------------------------
(* Invariants.                                                             *)

TypeOK ==
  /\ applied \in 0..Len(slots)
  /\ Len(slots) <= MaxSlots
  /\ leader \in Leaders
  /\ first \in 1..NextSeq
  /\ meta \in Leaders

\* The fence: a record of a uuid was judged under that uuid, and an unfenced
\* record in multi-writer mode, in both directions of a switch and across the
\* seal. The same for the accepted truncates.
Judged(l, u) == IF u = None THEN IsMulti(l) ELSE l = u
FenceTrue ==
  /\ \A s \in DOMAIN log : Judged(OracleAt(log[s].slot), log[s].u)
  /\ \A t \in truncs : Judged(OracleAt(t.slot), t.u)

\* Every write acknowledged (applied, or dup on a resend) is in the journal at
\* the seq of the answer.
AckTrue ==
  \A a \in answers :
    a.kind \in {"applied", "dup"} =>
      /\ a.seq \in DOMAIN log
      /\ log[a.seq].op = a.m.op
      /\ log[a.seq].u = IF a.m.kind = "f" THEN a.m.u ELSE None

\* A final refusal of a fenced write is true now and for ever: the write is
\* not in the journal as a fenced record. So a resend resolves an Unknown.
RefusalFinal ==
  \A a \in answers :
    (a.m.kind = "f" /\ a.kind \in {"fenced", "taken"}) =>
      \A s \in DOMAIN log : ~(log[s].u # None /\ log[s].op = a.m.op)

\* Single-writer mode is exactly-once: a write is at most once in the journal
\* as a fenced record, and never both as a fenced and an unfenced record.
ExactlyOnce ==
  \A s, t \in DOMAIN log :
    (s # t /\ log[s].op = log[t].op) => (log[s].u = None /\ log[t].u = None)

\* A writer that won a SetLeader never aims a write below the next_seq at
\* which it started to lead, the position that the switch left.
BasisFresh ==
  \A m \in sent : \A st \in starts :
    (m.kind = "f" /\ m.u = st.u) => m.e >= st.s

FirstMonotone == [][first' >= first]_vars

\* Not a rule: the limit of the multi-writer contract. A refusal of an
\* unfenced write is true only for that copy; another copy of the same
\* write can land later, after the mode went back to None.
UnfencedRefusalFinal ==
  \A a \in answers :
    (a.m.kind = "u" /\ a.kind = "wrongmode") =>
      \A s \in DOMAIN log : log[s].op # a.m.op

----------------------------------------------------------------------------
(* Reach: invariants that must break, so that the passing checks are not   *)
(* empty.                                                                  *)

\* A write applied before SetLeader(None, u) is answered dup on its resend
\* after it.
NoDupAfterSwitch == ~\E a \in answers : a.kind = "dup" /\ IsMulti(a.leader)

\* After SetLeader(u, None), a write of u is applied.
NoWriteAfterBack ==
  ~\E a \in answers, st \in starts :
     a.kind = "applied" /\ a.m.kind = "f" /\ IsMulti(st.from) /\ a.m.u = st.u

\* The Fence at the seal carries None, and a write is judged after it.
NoFenceNone ==
  ~\E a \in answers : SealAt > 0 /\ a.slot > SealAt
                      /\ IsMulti(slots[SealAt].leader) /\ a.kind = "applied"

\* A resend after the seal finds its write below the boundary.
NoBelow == ~\E a \in answers : a.kind = "below"

\* A resend of a fenced write is answered truncated after an unfenced
\* truncate.
NoTruncatedResend ==
  ~\E a \in answers, t \in truncs :
     a.kind = "truncated" /\ a.m.kind = "f" /\ t.u = None
=============================================================================

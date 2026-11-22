---------------------------- MODULE PlacementPort ----------------------------
(***************************************************************************)
(* The placement port between the grid controller of a journal and the    *)
(* cell (#1421 (the placement port), docs/architecture/04-roles.md §4.4,  *)
(* docs/journal/06-controller.md §J6). A coarse model: no Paxos inside.   *)
(* A journal has members; a reshape swaps its members in one step, and    *)
(* the journal's own models (`grid-controller/`, `re-replication/`) judge *)
(* that step. A journal is writable while a majority of its members is up.*)
(*                                                                         *)
(* The grid controller of a journal owns the loop that changes its        *)
(* membership: ask the cell, reshape, release. The cell owns the answers: *)
(* which member must leave (its machine is gone or drains), where a new   *)
(* member goes, and how many moves run at one time.                       *)
(*                                                                         *)
(* The calls (Pierre, 2026-11-22, question 1 of #1421):                 *)
(*   standing     the cell's records `gone` and `draining`, read by the   *)
(*                controller to find the members that must leave.          *)
(*   place(rid, journal, keep, leave, count)                               *)
(*                one grant, all or none, keyed by a request id that the  *)
(*                caller draws. The cell writes the grant and its         *)
(*                bookings in one write of its control journal, so the id *)
(*                is in the fold before any machine is booked. The same   *)
(*                rid gets the same answer. "busy" (the limit of moves in *)
(*                flight is reached) writes nothing; the caller asks again *)
(*                with the same rid. "refused" (no machine fits) is       *)
(*                recorded; the caller asks later with a new rid.          *)
(*   release(rid, members)  the move is over: the cell ends the grant and *)
(*                frees its bookings of the machines that are not members *)
(*                of the journal now. After a reshape these are the       *)
(*                members that left; after an abandon (the journal is     *)
(*                below a majority and cannot reshape) the new machines.  *)
(*                A grant in flight is the token of the rate limit of the *)
(*                cell, so every grant must end.                          *)
(*   A refusal: the controller asks again, with a new rid, only once the  *)
(*   cell's records (gone, draining, bookings) have changed.              *)
(*                                                                         *)
(* The cell's own control journal (Pierre, 2026-11-22, question 2 of     *)
(* #1421): it is one of `Journals` (`Cell`) and asks the same port.       *)
(* Every step of the cell is a write of that journal, so each needs a     *)
(* majority of its members up, its own heal too.                          *)
(*                                                                         *)
(* The faults: machines that fail for good (the cell records them gone),  *)
(* a drain by an operator, lost and repeated messages, a restart of the   *)
(* cell coordinator (its memory is lost, its journal stays). A crash or a *)
(* takeover of a grid controller is #1422, not here: the controller of a  *)
(* journal stays up.                                                       *)
(*                                                                         *)
(* The constants that select a control (each must break its property):    *)
(*   IdFirst = FALSE    the cell books the machines in one write and      *)
(*                      writes the grant in a second write: a restart     *)
(*                      between the two leaves bookings nobody frees.      *)
(*   Dedup = FALSE      the cell does not look up the rid: a repeated     *)
(*                      request gets a second grant.                      *)
(*   AllOrNone = FALSE  the cell grants fewer machines than asked.        *)
(*   CheckLimit = FALSE the cell grants past the limit of moves in flight.*)
(*   FreeOnRelease = FALSE  a release ends the grant but keeps the        *)
(*                      bookings of the machines that are not members.    *)
(*   CanAbandon = FALSE a controller whose journal is below a majority    *)
(*                      keeps its grant: the token of the cell is lost.   *)
(*   CellPlace = "fixed"    the cell's control journal does not use the   *)
(*                      port: it stays on its first machines.             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Machines,     \* the machines of the cell
          Journals,     \* the journals, the cell's control journal too
          Cell,         \* the cell's control journal, in Journals
          Mem0,         \* Mem0[j]: the first members of journal j
          Cap,          \* the roles that one machine holds at most
          Limit,        \* the moves in flight in the cell at most
          MaxRid,       \* the request ids that one controller draws
          MaxFail, MaxDrain, MaxLoss, MaxCrash,
          IdFirst, Dedup, AllOrNone, CheckLimit, FreeOnRelease,
          CanAbandon, CellPlace

ASSUME Cell \in Journals
ASSUME \A j \in Journals : Mem0[j] \subseteq Machines
ASSUME CellPlace \in {"self", "fixed"}

N(j) == Cardinality(Mem0[j])
Rid == Journals \X (1 .. MaxRid)

VARIABLES members,   \* members[j]: the members of journal j now
          down,      \* machines that failed (a fact, not a record)
          gone,      \* the cell's durable record: machines gone for good
          draining,  \* the cell's durable record: machines that drain
          grants,    \* the cell's durable grants, one record each
          bookings,  \* the cell's durable bookings [j, m, rid]
          staged,    \* the cell coordinator's memory (IdFirst = FALSE)
          ctl,       \* ctl[j]: the grid controller of journal j
          net,       \* the messages in flight (a set: repeats are free)
          faults     \* counts of each fault, for the bounds

vars == <<members, down, gone, draining, grants, bookings, staged, ctl, net, faults>>

-----------------------------------------------------------------------------
Majority(S) == 2 * Cardinality(S \cap (Machines \ down)) > Cardinality(S)

\* Every step of the cell is a write of its control journal.
CellUp == Majority(members[Cell])

Leaving == gone \cup draining

Active == {g \in grants : g.state = "active"}

BookedOn(m) == {b \in bookings : b.m = m}

\* A machine that may take a new member of journal j that keeps `keep`.
Free(j, keep) == {m \in Machines \ (Leaving \cup keep) :
                    Cardinality(BookedOn(m)) < Cap}

Req(rid, j, keep, leave) ==
    [t |-> "place", rid |-> rid, j |-> j, keep |-> keep, leave |-> leave]

Requests == {r \in net : r.t = "place"}
Releases == {r \in net : r.t = "release"}

Send(msgs) == net' = net \cup msgs

-----------------------------------------------------------------------------
Init ==
    /\ members = Mem0
    /\ down = {}
    /\ gone = {}
    /\ draining = {}
    /\ grants = {}
    /\ bookings = UNION {{[j |-> j, m |-> m, rid |-> <<j, 0>>] : m \in Mem0[j]} : j \in Journals}
    /\ staged = {}
    /\ ctl = [j \in Journals |-> [ph |-> "idle", n |-> 0, leave |-> {}, add |-> {}, seen |-> <<>>,
                                     rin |-> {}]]
    /\ net = {}
    /\ faults = [fail |-> 0, drain |-> 0, loss |-> 0, crash |-> 0]

-----------------------------------------------------------------------------
(* Faults and the operator. *)

Fail(m) ==
    /\ faults.fail < MaxFail
    /\ down' = down \cup {m}
    /\ faults' = [faults EXCEPT !.fail = @ + 1]
    /\ UNCHANGED <<members, gone, draining, grants, bookings, staged, ctl, net>>

\* `cell drain-machine`: a record of the cell.
Drain(m) ==
    /\ CellUp
    /\ faults.drain < MaxDrain
    /\ draining' = draining \cup {m}
    /\ faults' = [faults EXCEPT !.drain = @ + 1]
    /\ UNCHANGED <<members, down, gone, grants, bookings, staged, ctl, net>>

Lose(msg) ==
    /\ faults.loss < MaxLoss
    /\ net' = net \ {msg}
    /\ faults' = [faults EXCEPT !.loss = @ + 1]
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, ctl>>

\* The cell coordinator restarts (or a new term starts): its memory is lost.
CellCrash ==
    /\ faults.crash < MaxCrash
    /\ staged' = {}
    /\ faults' = [faults EXCEPT !.crash = @ + 1]
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, ctl, net>>

-----------------------------------------------------------------------------
(* The cell: the failure detector and the answers of the port. *)

\* "Gone" is a durable record of the cell, not the memory of a term.
RecordGone(m) ==
    /\ CellUp
    /\ m \in down \ gone
    /\ gone' = gone \cup {m}
    /\ UNCHANGED <<members, down, draining, grants, bookings, staged, ctl, net, faults>>

\* The sets that a grant may book for the request `r`.
Choices(r) ==
    {S \in SUBSET Free(r.j, r.keep) :
        IF AllOrNone THEN Cardinality(S) = Cardinality(r.leave)
        ELSE S # {} /\ Cardinality(S) <= Cardinality(r.leave)}

Grant(r, S) == [rid |-> r.rid, j |-> r.j, leave |-> r.leave, add |-> S, state |-> "active"]

Book(r, S) == {[j |-> r.j, m |-> m, rid |-> r.rid] : m \in S}

Busy == CheckLimit /\ Cardinality(Active) + Cardinality(staged) >= Limit

\* The answer of a grant again, at any time: a repeated request gets the same
\* answer. It writes nothing and needs no majority: a grant in the fold is
\* durable and never changes its answer, so an answer from an old read of the
\* fold is still true.
PlaceAgain(g) ==
    /\ Send({IF g.state = "refused"
             THEN [t |-> "refused", rid |-> g.rid]
             ELSE [t |-> "granted", rid |-> g.rid, add |-> g.add]})
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, ctl, faults>>

New(r) ==
    /\ ~(Dedup /\ \E g \in grants : g.rid = r.rid)
    /\ ~(\E p \in staged : p.rid = r.rid)

\* While the limit of moves in flight is reached, the cell answers "busy" and
\* writes nothing: in the model it takes no step, and the request waits.

\* The grant and its bookings in one write of the cell's control journal.
PlaceGrant(r) ==
    /\ CellUp
    /\ New(r)
    /\ ~Busy
    /\ \E S \in Choices(r) :
         IF IdFirst
         THEN /\ grants' = grants \cup {Grant(r, S)}
              /\ bookings' = bookings \cup Book(r, S)
              /\ Send({[t |-> "granted", rid |-> r.rid, add |-> S]})
              /\ UNCHANGED staged
         ELSE \* The control: the bookings first, the grant in a later write.
              /\ bookings' = bookings \cup Book(r, S)
              /\ staged' = staged \cup {Grant(r, S)}
              /\ UNCHANGED <<grants, net>>
    /\ UNCHANGED <<members, down, gone, draining, ctl, faults>>

\* The second write of the control IdFirst = FALSE.
CommitStaged(p) ==
    /\ CellUp
    /\ grants' = grants \cup {p}
    /\ staged' = staged \ {p}
    /\ Send({[t |-> "granted", rid |-> p.rid, add |-> p.add]})
    /\ UNCHANGED <<members, down, gone, draining, bookings, ctl, faults>>

\* No machine fits: the refusal is recorded under the same rid, at the limit
\* or not.
PlaceRefuse(r) ==
    /\ CellUp
    /\ New(r)
    /\ Choices(r) = {}
    /\ grants' = grants \cup {[rid |-> r.rid, j |-> r.j, leave |-> r.leave,
                              add |-> {}, state |-> "refused"]}
    /\ Send({[t |-> "refused", rid |-> r.rid]})
    /\ UNCHANGED <<members, down, gone, draining, bookings, staged, ctl, faults>>

\* release(rid, members): end the grant and free its bookings of the machines
\* that are not members now.
Release(msg) ==
    /\ CellUp
    /\ \E g \in grants :
         /\ g.rid = msg.rid
         /\ grants' = (grants \ {g}) \cup {[g EXCEPT !.state = "done"]}
         /\ bookings' =
              IF FreeOnRelease /\ g.state = "active"
              THEN {b \in bookings : ~(b.j = g.j /\ b.m \in (g.add \cup g.leave) \ msg.in)}
              ELSE bookings
    /\ Send({[t |-> "released", rid |-> msg.rid]})
    /\ UNCHANGED <<members, down, gone, draining, staged, ctl, faults>>

-----------------------------------------------------------------------------
(* The grid controller of journal j: ask, reshape, release. *)

Uses(j) == j # Cell \/ CellPlace = "self"

RidOf(j) == <<j, ctl[j].n>>

\* The records of the cell that a refusal reads.
Seen == <<gone, draining, bookings>>

Ask(j) ==
    LET L == members[j] \cap Leaving
    IN /\ Uses(j)
       /\ \/ ctl[j].ph = "idle"
          \/ ctl[j].ph = "refused" /\ ctl[j].seen # Seen
       /\ L # {}
       /\ ctl[j].n < MaxRid
       /\ ctl' = [ctl EXCEPT ![j] = [ph |-> "asked", n |-> @.n + 1, leave |-> L, add |-> {},
                                     seen |-> <<>>, rin |-> {}]]
       /\ Send({Req(<<j, ctl[j].n + 1>>, j, members[j] \ L, L)})
       /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, faults>>

\* Ask again with the same rid: the request or its answer may be lost, or the
\* cell was busy.
AskAgain(j) ==
    LET r == Req(RidOf(j), j, members[j] \ ctl[j].leave, ctl[j].leave)
    IN /\ ctl[j].ph = "asked"
       /\ Send({r})
       /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, ctl, faults>>

\* A rid that got a refusal never gets a grant (OneGrantPerRid), so the
\* refusal finds the controller asking. It waits for a change of the records.
GotRefused(j) ==
    /\ [t |-> "refused", rid |-> RidOf(j)] \in net
    /\ ctl' = [ctl EXCEPT ![j].ph = "refused", ![j].seen = Seen]
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, net, faults>>

GotGrant(j) ==
    /\ ctl[j].ph = "asked"
    /\ \E a \in net :
         /\ a.t = "granted" /\ a.rid = RidOf(j)
         /\ ctl' = [ctl EXCEPT ![j].ph = "reshape", ![j].add = a.add]
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, net, faults>>

\* One reshape: the new members are in when the leaving ones are out.
Reshape(j) ==
    /\ ctl[j].ph = "reshape"
    /\ Majority(members[j])
    /\ members' = [members EXCEPT ![j] = (@ \ ctl[j].leave) \cup ctl[j].add]
    /\ ctl' = [ctl EXCEPT ![j].ph = "release", ![j].rin = members'[j]]
    /\ Send({[t |-> "release", rid |-> RidOf(j), in |-> members'[j]]})
    /\ UNCHANGED <<down, gone, draining, grants, bookings, staged, faults>>

\* The journal is below a majority, so no reshape can be written: the
\* controller holds and pages, and ends its grant so that the token of the
\* cell is free for the other journals.
Abandon(j) ==
    /\ CanAbandon
    /\ ctl[j].ph = "reshape"
    /\ ~Majority(members[j])
    /\ ctl' = [ctl EXCEPT ![j].ph = "release", ![j].rin = members[j]]
    /\ Send({[t |-> "release", rid |-> RidOf(j), in |-> members[j]]})
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, faults>>

ReleaseAgain(j) ==
    /\ ctl[j].ph = "release"
    /\ Send({[t |-> "release", rid |-> RidOf(j), in |-> ctl[j].rin]})
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, ctl, faults>>

\* Only a release sends "released", so it finds the controller releasing, or
\* idle already.
GotReleased(j) ==
    /\ [t |-> "released", rid |-> RidOf(j)] \in net
    /\ ctl' = [ctl EXCEPT ![j] = [ph |-> "idle", n |-> @.n, leave |-> {}, add |-> {}, seen |-> <<>>,
                                 rin |-> {}]]
    /\ UNCHANGED <<members, down, gone, draining, grants, bookings, staged, net, faults>>

-----------------------------------------------------------------------------
CellStep ==
    \/ \E m \in Machines : RecordGone(m)
    \/ \E g \in grants : PlaceAgain(g)
    \/ \E r \in Requests : PlaceGrant(r) \/ PlaceRefuse(r)
    \/ \E r \in Releases : Release(r)
    \/ \E p \in staged : CommitStaged(p)

CtlStep(j) ==
    \/ Ask(j) \/ AskAgain(j) \/ GotRefused(j) \/ GotGrant(j)
    \/ Reshape(j) \/ Abandon(j) \/ ReleaseAgain(j) \/ GotReleased(j)

Fault ==
    \/ \E m \in Machines : Fail(m) \/ Drain(m)
    \/ \E msg \in net : Lose(msg)
    \/ CellCrash

Next == CellStep \/ (\E j \in Journals : CtlStep(j)) \/ Fault

Spec == Init /\ [][Next]_vars

\* Faults are bounded and take no fairness; every step of the cell and of a
\* controller does.
Fairness ==
    /\ \A m \in Machines : WF_vars(RecordGone(m))
    /\ WF_vars(\E g \in grants : PlaceAgain(g))
    /\ WF_vars(\E r \in Requests : PlaceGrant(r) \/ PlaceRefuse(r))
    /\ WF_vars(\E r \in Releases : Release(r))
    /\ WF_vars(\E p \in staged : CommitStaged(p))
    /\ \A j \in Journals : WF_vars(CtlStep(j))

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties. *)

TypeOK ==
    /\ \A j \in Journals : members[j] \subseteq Machines
    /\ down \subseteq Machines /\ gone \subseteq down
    /\ \A g \in grants : g.state \in {"active", "refused", "done"} /\ g.rid \in Rid
    /\ \A b \in bookings : b.j \in Journals /\ b.m \in Machines

\* A grant with the same request id is the same grant.
OneGrantPerRid == \A g, h \in grants : g.rid = h.rid => g = h

\* A grant is all or none.
GrantWhole == \A g \in grants : g.state # "refused" => Cardinality(g.add) = Cardinality(g.leave)

\* The grants in flight never exceed the limit, and neither do the journals
\* that reshape on a grant.
InFlight == Cardinality(Active) <= Limit

MovesInFlight == Cardinality({j \in Journals : ctl[j].ph = "reshape"}) <= Limit

\* A machine holds at most Cap roles.
Capacity == \A m \in Machines : Cardinality(BookedOn(m)) <= Cap

\* A member leaves only after its replacement is in: a journal keeps its size.
KeepsSize == \A j \in Journals : Cardinality(members[j]) = N(j)

\* Each member has its booking.
MembersBooked == \A j \in Journals : \A m \in members[j] :
                   \E b \in bookings : b.j = j /\ b.m = m

\* Each booking belongs to a member, to a move in flight, or to a write that
\* the cell coordinator still holds in memory: nothing leaks, after an
\* abandon too.
NoOrphan ==
    \A b \in bookings :
      \/ b.m \in members[b.j]
      \/ \E g \in Active : g.j = b.j /\ b.m \in g.add \cup g.leave
      \/ \E p \in staged : p.rid = b.rid

\* A released booking is free again: once a grant ends, no booking holds a
\* machine of the grant for its journal unless the machine is a member or a
\* later move holds it.
ReleasedFree ==
    \A g \in grants : g.state = "done" =>
      \A m \in g.add \cup g.leave :
        (/\ m \notin members[g.j]
         /\ ~\E h \in Active : h.j = g.j /\ m \in h.add \cup h.leave
         /\ ~\E p \in staged : p.j = g.j /\ m \in p.add)
          => ~\E b \in bookings : b.j = g.j /\ b.m = m

\* Every write of the cell is a write of its control journal, and every change
\* of a journal's members is a reshape of that journal: each needs a majority
\* of the members up.
Writes ==
    [][/\ (<<gone, draining, grants, bookings>>' # <<gone, draining, grants, bookings>>) => CellUp
       /\ \A j \in Journals : members'[j] # members[j] => Majority(members[j])]_vars

Safety == TypeOK /\ OneGrantPerRid /\ GrantWhole /\ InFlight /\ MovesInFlight /\ Capacity /\ KeepsSize
          /\ MembersBooked /\ NoOrphan /\ ReleasedFree

\* A journal is settled when it is off the machines that failed or drain, or
\* when it reports a true reason: it is below a majority (hold and page), or
\* the cell refused its move and nothing changed since.
Settled(j) ==
    \/ members[j] \cap (down \cup draining) = {}
    \/ ~Majority(members[j])
    \/ ctl[j].ph = "refused" /\ ctl[j].seen = Seen

\* Once the faults stop, every move is done and every journal is settled: the
\* cell heals with nobody watching. A cell below a majority writes nothing,
\* so nothing heals: it holds and pages.
Healed ==
    <>[](CellUp => /\ Active = {}
                   /\ \A j \in Journals : Settled(j) /\ ctl[j].ph \in {"idle", "refused"})

-----------------------------------------------------------------------------
(* Reachability: each must be violated, so the model reaches the case. *)

NoCellHeal == ~\E g \in grants : g.j = Cell /\ g.state = "done"
NoBusy == ~\E j \in Journals : /\ ctl[j].ph = "asked"
                               /\ Cardinality(Active) >= Limit
                               /\ ~\E g \in grants : g.rid = RidOf(j)
NoRefusal == ~\E g \in grants : g.state = "refused"
NoAbandon == ~\E j \in Journals : ctl[j].ph = "release" /\ ~Majority(members[j])
NoTwoMoves == ~\E g, h \in grants : g.j # h.j /\ g.state = "done" /\ h.state = "done"
=============================================================================

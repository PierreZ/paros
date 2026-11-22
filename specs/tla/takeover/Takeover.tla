------------------------------ MODULE Takeover ------------------------------
(***************************************************************************)
(* The takeover by a new controller, for #1422: what a coordinator knows   *)
(* and does when it wins a term after another one                         *)
(* (docs/architecture/04-roles.md §4.4, §3.3). The term itself is the     *)
(* election of `election/`, taken here as one atomic step.                 *)
(*                                                                         *)
(* A coordinator books a role for a request on a machine (a record in its *)
(* control journal, written under its term: the journal refuses the write *)
(* of a deposed coordinator, which then stops), then orders the machine to *)
(* host it with `Assign(request, term)`. A machine registers with the     *)
(* coordinator of each term that it learns from the journal: the          *)
(* registration names its incarnation and the roles it hosts. The        *)
(* machine sends it again until the coordinator acks it; the model takes  *)
(* that as a registration that the network never loses. A machine         *)
(* answers an `Assign` that took effect with an ack, and an `Assign` it    *)
(* refuses with a nack. A machine has a new reference at each             *)
(* incarnation (a dynamic reference of moonpool RPC; the model takes it   *)
(* as given, not as a rule): an order to an incarnation that is gone      *)
(* comes back to its sender as an error (`gone`), and the sender asks the *)
(* fixed directory of the machine for its current registration. A         *)
(* coordinator that wins a term builds its view of the machines from the  *)
(* registrations and acks of its term; the bookings come from the journal. *)
(* It trusts its view in one step: it frees a request that it believes no *)
(* machine hosts.                                                          *)
(*                                                                         *)
(* The rules of #1422, as constants (TRUE is the rule):                    *)
(*   OneWrite      the machine checks the term of an order and does its   *)
(*                 effect in one atomic write (FALSE: two writes, a check *)
(*                 then the effect).                                       *)
(*   Idem          `Assign` is idempotent by its request id.              *)
(*   ClearView     a new term starts with an empty view, never with the   *)
(*                 memory of an earlier term.                              *)
(*   CoordRef      a coordinator has a new reference at each term: a      *)
(*                 message of an earlier term never reaches the next term *)
(*                 of the same coordinator through a fixed endpoint.       *)
(*   IdInBooking   a booking names the machine by its durable id; the     *)
(*                 reference of an order comes from the view of the term  *)
(*                 (FALSE: the booking keeps the reference it saw).        *)
(*   RegFence      a registration with term t raises the machine's        *)
(*                 highest term to t in the same write.                    *)
(*   NackStops     a refused order tells its coordinator that a newer     *)
(*                 term began, and the coordinator stops.                  *)
(*   GoneAsks      a `gone` error marks the machine as unknown in the     *)
(*                 view of its coordinator, which asks the fixed          *)
(*                 directory of the machine for its current registration. *)
(*                                                                         *)
(* The faults: a coordinator is deposed and keeps sending orders of its   *)
(* old term; a coordinator crashes and loses its memory (it must win a    *)
(* new term to act again); messages are late, reordered, sent two times   *)
(* and lost; a machine restarts as a new incarnation (its disk stays).     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Coords, Machines, Requests,
          MaxTerms, MaxRestarts, MaxCrashes, MaxDups, MaxLoss,
          OneWrite, Idem, ClearView, CoordRef, IdInBooking, RegFence,
          NackStops, GoneAsks

NoView == [ok |-> FALSE, inc |-> 0, roles |-> {}]
AnyMachine == CHOOSE m \in Machines : TRUE

VARIABLES term, holder,  \* the term of the control journal and its holder
          cterm,         \* cterm[c]: the term c believes it holds, 0 for none
          view,          \* view[c][m]: the registration of m that c keeps
          acked,         \* acked[c]: the requests acked to c in its term
          sentQ,         \* sentQ[c]: the requests c ordered in its term
          booked,        \* the requests booked in the control journal
          freed,         \* the requests freed in the control journal
          bookM,         \* bookM[q]: the durable id of the machine of q
          bookRef,       \* bookRef[q]: the incarnation the booking of q saw
          inc,           \* inc[m]: the incarnation of m
          seen,          \* seen[m]: the highest term m wrote on its disk
          done,          \* done[m]: the requests that took effect on m, on disk
          count,         \* count[q]: how many times the effect of q took place
          mterm,         \* mterm[m]: the term m registered with, in memory
          pend,          \* pend[m]: orders checked and not done (OneWrite = FALSE)
          net,
          restarts, crashes, dups, lost,
          late           \* an effect of a term took place after m wrote a higher term

vars == <<term, holder, cterm, view, acked, sentQ, booked, freed, bookM, bookRef, inc, seen, done,
          count, mterm, pend, net, restarts, crashes, dups, lost, late>>
coordVars == <<cterm, view, acked, sentQ>>
bookVars == <<booked, freed, bookM, bookRef>>
machVars == <<inc, seen, done, count, mterm, pend>>
budgets == <<restarts, crashes, dups, lost>>

Blank == [k |-> "", q |-> CHOOSE q \in Requests : TRUE, t |-> 0, m |-> AnyMachine,
          inc |-> 0, c |-> CHOOSE c \in Coords : TRUE, roles |-> {}]

Init ==
  /\ term = 1 /\ holder = CHOOSE c \in Coords : TRUE
  /\ cterm = [c \in Coords |-> IF c = holder THEN 1 ELSE 0]
  /\ view = [c \in Coords |-> [m \in Machines |-> NoView]]
  /\ acked = [c \in Coords |-> {}] /\ sentQ = [c \in Coords |-> {}]
  /\ booked = {} /\ freed = {} /\ bookM = [q \in Requests |-> AnyMachine]
  /\ bookRef = [q \in Requests |-> 0]
  /\ inc = [m \in Machines |-> 1] /\ seen = [m \in Machines |-> 0]
  /\ done = [m \in Machines |-> {}] /\ count = [q \in Requests |-> 0]
  /\ mterm = [m \in Machines |-> 0]
  /\ pend = [m \in Machines |-> {}]
  /\ net = {} /\ restarts = 0 /\ crashes = 0 /\ dups = 0 /\ lost = 0 /\ late = FALSE

\* A message is taken from the network; it may stay as a copy (a duplicate).
Take(msg) == \/ net' = net \ {msg} /\ UNCHANGED dups
             \/ dups < MaxDups /\ dups' = dups + 1 /\ UNCHANGED net

\* The same, and an answer goes out.
TakeSend(msg, ans) ==
  \/ net' = (net \ {msg}) \cup {ans} /\ UNCHANGED dups
  \/ dups < MaxDups /\ dups' = dups + 1 /\ net' = net \cup {ans}

-----------------------------------------------------------------------------
\* The coordinators.

\* A coordinator wins the next term (the election of `election/`).
Win(c) ==
  /\ term < MaxTerms
  /\ term' = term + 1 /\ holder' = c
  /\ cterm' = [cterm EXCEPT ![c] = term + 1]
  /\ IF ClearView
     THEN /\ view' = [view EXCEPT ![c] = [m \in Machines |-> NoView]]
          /\ acked' = [acked EXCEPT ![c] = {}]
          /\ sentQ' = [sentQ EXCEPT ![c] = {}]
     ELSE UNCHANGED <<view, acked, sentQ>>
  /\ UNCHANGED <<bookVars, machVars, net, budgets, late>>

\* A coordinator books a request on a machine that its view shows: a write
\* in the control journal under its term. The journal refuses the write of
\* a coordinator whose term ended, and that coordinator stops (`election/`).
Book(c, q, m) ==
  /\ q \notin booked \cup freed /\ view[c][m].ok
  /\ IF cterm[c] = term
     THEN /\ booked' = booked \cup {q}
          /\ bookM' = [bookM EXCEPT ![q] = m]
          /\ bookRef' = [bookRef EXCEPT ![q] = view[c][m].inc]
          /\ UNCHANGED <<cterm, freed>>
     ELSE /\ cterm' = [cterm EXCEPT ![c] = 0]
          /\ UNCHANGED bookVars
  /\ UNCHANGED <<term, holder, view, acked, sentQ, machVars, net, budgets, late>>

\* A coordinator frees a booked request that it believes no machine hosts:
\* its view shows the machine, the registration does not name the role, no
\* ack of its term names it, and it sent no order for it in its term. This
\* is the step that trusts the view; the rules make that trust safe. The
\* journal refuses the write of a deposed coordinator, which stops.
Free(c, q) ==
  LET m == bookM[q] IN
  /\ q \in booked /\ view[c][m].ok
  /\ q \notin view[c][m].roles \cup acked[c] \cup sentQ[c]
  /\ IF cterm[c] = term
     THEN /\ booked' = booked \ {q}
          /\ freed' = freed \cup {q}
          /\ UNCHANGED cterm
     ELSE /\ cterm' = [cterm EXCEPT ![c] = 0]
          /\ UNCHANGED <<booked, freed>>
  /\ UNCHANGED <<term, holder, view, acked, sentQ, bookM, bookRef, machVars, net, budgets,
                 late>>

\* A coordinator that believes it holds a term orders each booked request
\* that its view does not show done. It orders again while no ack came,
\* since an order can be lost. A deposed coordinator does this too, with
\* its old term.
Order(c, q) ==
  LET m == bookM[q] IN
  LET ref == IF IdInBooking THEN view[c][m].inc ELSE bookRef[q] IN
  /\ cterm[c] > 0 /\ q \in booked /\ view[c][m].ok
  /\ q \notin view[c][m].roles \cup acked[c]
  /\ sentQ' = [sentQ EXCEPT ![c] = sentQ[c] \cup {q}]
  /\ net' = net \cup {[Blank EXCEPT !.k = "assign", !.q = q, !.t = cterm[c], !.m = m,
                                    !.inc = ref, !.c = c]}
  /\ UNCHANGED <<term, holder, cterm, view, acked, bookVars, machVars, budgets, late>>

\* A message to a coordinator reaches it only if it names its current
\* reference: its term (CoordRef). Else, a fixed endpoint takes it.
Reaches(c, msg) == msg.c = c /\ (CoordRef => msg.t = cterm[c])

RecvReg(c) ==
  \E msg \in net :
    /\ msg.k = "reg" /\ Reaches(c, msg)
    /\ Take(msg)
    /\ view' = [view EXCEPT ![c][msg.m] = [ok |-> TRUE, inc |-> msg.inc, roles |-> msg.roles]]
    /\ UNCHANGED <<term, holder, cterm, acked, sentQ, bookVars, machVars, restarts,
                   crashes, lost, late>>

RecvAck(c) ==
  \E msg \in net :
    /\ msg.k = "ack" /\ Reaches(c, msg)
    /\ Take(msg)
    /\ acked' = [acked EXCEPT ![c] = acked[c] \cup {msg.q}]
    /\ UNCHANGED <<term, holder, cterm, view, sentQ, bookVars, machVars, restarts,
                   crashes, lost, late>>

RecvNack(c) ==
  \E msg \in net :
    /\ msg.k = "nack" /\ Reaches(c, msg)
    /\ net' = net \ {msg}
    /\ cterm' = IF NackStops THEN [cterm EXCEPT ![c] = 0] ELSE cterm
    /\ UNCHANGED <<term, holder, view, acked, sentQ, bookVars, machVars, budgets, late>>

\* With GoneAsks, a `gone` for the incarnation in the view marks the
\* machine as unknown and asks the fixed directory of the machine for its
\* current registration.
RecvGone(c) ==
  \E msg \in net :
    /\ msg.k = "gone" /\ Reaches(c, msg)
    /\ IF GoneAsks /\ view[c][msg.m].inc = msg.inc
       THEN /\ view' = [view EXCEPT ![c][msg.m].ok = FALSE]
            /\ net' = (net \ {msg}) \cup
                      {[Blank EXCEPT !.k = "ask", !.m = msg.m, !.c = c, !.t = cterm[c]]}
       ELSE /\ net' = net \ {msg}
            /\ UNCHANGED view
    /\ UNCHANGED <<term, holder, cterm, acked, sentQ, bookVars, machVars, budgets, late>>

\* A coordinator crashes: its memory is lost. It acts again only after it
\* wins a new term. The bound keeps a term for the recovery.
Crash(c) ==
  /\ crashes < MaxCrashes /\ term < MaxTerms
  /\ crashes' = crashes + 1
  /\ cterm' = [cterm EXCEPT ![c] = 0]
  /\ view' = [view EXCEPT ![c] = [m \in Machines |-> NoView]]
  /\ acked' = [acked EXCEPT ![c] = {}]
  /\ sentQ' = [sentQ EXCEPT ![c] = {}]
  /\ UNCHANGED <<term, holder, bookVars, machVars, net, restarts, dups, lost, late>>

\* The messages to a coordinator: every kind but an order and an ask.
ToCoord == {msg \in net : msg.k \notin {"assign", "ask"}}

\* A message to a coordinator activation that is gone is lost.
Drop ==
  \E msg \in ToCoord :
    /\ CoordRef /\ msg.t # cterm[msg.c]
    /\ net' = net \ {msg}
    /\ UNCHANGED <<term, holder, coordVars, bookVars, machVars, budgets, late>>

\* A message to a machine incarnation that is gone: an order comes back to
\* its coordinator as `gone`.
Bounce ==
  \E msg \in net :
    /\ msg.k = "assign" /\ msg.inc # inc[msg.m]
    /\ net' = (net \ {msg}) \cup {[msg EXCEPT !.k = "gone"]}
    /\ UNCHANGED <<term, holder, coordVars, bookVars, machVars, budgets, late>>

\* The network loses a message. A registration and an ask are sent again
\* until they are answered, so they are not lost.
Lose ==
  /\ lost < MaxLoss
  /\ \E msg \in net : msg.k \notin {"reg", "ask"} /\ net' = net \ {msg}
  /\ lost' = lost + 1
  /\ UNCHANGED <<term, holder, coordVars, bookVars, machVars, restarts, crashes, dups,
                 late>>

-----------------------------------------------------------------------------
\* The machines.

\* A machine learns a new term from the control journal and registers with
\* its holder: its incarnation and the requests it hosts. With RegFence the
\* same write raises its highest term.
Register(m) ==
  /\ mterm[m] < term
  /\ mterm' = [mterm EXCEPT ![m] = term]
  /\ seen' = IF RegFence /\ term > seen[m] THEN [seen EXCEPT ![m] = term] ELSE seen
  /\ net' = net \cup {[Blank EXCEPT !.k = "reg", !.m = m, !.inc = inc[m], !.c = holder,
                                    !.t = term, !.roles = done[m]]}
  /\ UNCHANGED <<term, holder, coordVars, bookVars, inc, done, count, pend, budgets, late>>

\* The fixed directory of a machine answers an ask with a registration of
\* the current incarnation, under the asker's term. With RegFence the same
\* write raises the machine's highest term.
AnswerAsk(m) ==
  \E msg \in net :
    /\ msg.k = "ask" /\ msg.m = m
    /\ TakeSend(msg, [Blank EXCEPT !.k = "reg", !.m = m, !.inc = inc[m], !.c = msg.c,
                                   !.t = msg.t, !.roles = done[m]])
    /\ seen' = IF RegFence /\ msg.t > seen[m] THEN [seen EXCEPT ![m] = msg.t] ELSE seen
    /\ UNCHANGED <<term, holder, coordVars, bookVars, inc, done, count, mterm, pend,
                   restarts, crashes, lost, late>>

\* The effect of an order: the role of the request is hosted (once, by Idem),
\* and the ack goes to the coordinator of the order's term.
Effect(m, msg) ==
  /\ late' = (late \/ msg.t < seen[m])
  /\ seen' = [seen EXCEPT ![m] = IF msg.t > seen[m] THEN msg.t ELSE seen[m]]
  /\ IF Idem /\ msg.q \in done[m]
     THEN UNCHANGED count
     ELSE count' = [count EXCEPT ![msg.q] = count[msg.q] + 1]
  /\ done' = [done EXCEPT ![m] = done[m] \cup {msg.q}]

Ack(msg) == [Blank EXCEPT !.k = "ack", !.q = msg.q, !.t = msg.t, !.m = msg.m, !.c = msg.c]

\* An order reaches the incarnation it names. The machine refuses
\* an order below its highest term. With OneWrite, the check and the effect
\* are one write; else the check passes now and the effect comes later.
Deliver(m) ==
  \E msg \in net :
    /\ msg.k = "assign" /\ msg.m = m /\ msg.inc = inc[m]
    /\ IF msg.t < seen[m]
       THEN /\ TakeSend(msg, [Ack(msg) EXCEPT !.k = "nack"])
            /\ UNCHANGED <<seen, done, count, pend, late>>
       ELSE IF OneWrite
            THEN /\ Effect(m, msg)
                 /\ TakeSend(msg, Ack(msg))
                 /\ UNCHANGED pend
            ELSE /\ pend' = [pend EXCEPT ![m] = pend[m] \cup {msg}]
                 /\ Take(msg)
                 /\ UNCHANGED <<seen, done, count, late>>
    /\ UNCHANGED <<term, holder, coordVars, bookVars, inc, mterm, restarts, crashes, lost>>

\* OneWrite = FALSE: the effect of a checked order, in a second write.
Apply(m) ==
  \E msg \in pend[m] :
    /\ pend' = [pend EXCEPT ![m] = pend[m] \ {msg}]
    /\ Effect(m, msg)
    /\ net' = net \cup {Ack(msg)}
    /\ UNCHANGED <<term, holder, coordVars, bookVars, inc, mterm, budgets>>

\* A machine restarts: its memory is lost, its disk stays.
Restart(m) ==
  /\ restarts < MaxRestarts
  /\ restarts' = restarts + 1
  /\ inc' = [inc EXCEPT ![m] = inc[m] + 1]
  /\ mterm' = [mterm EXCEPT ![m] = 0]
  /\ pend' = [pend EXCEPT ![m] = {}]
  /\ UNCHANGED <<term, holder, coordVars, bookVars, seen, done, count, net, crashes, dups,
                 lost, late>>

-----------------------------------------------------------------------------
\* A holder that crashed is replaced: some coordinator wins the next term.
Recover == cterm[holder] = 0 /\ \E c \in Coords : Win(c)

Next ==
  \/ \E c \in Coords :
       \/ Win(c) \/ Crash(c) \/ RecvReg(c) \/ RecvAck(c) \/ RecvNack(c) \/ RecvGone(c)
       \/ \E q \in Requests : Order(c, q) \/ Free(c, q) \/ \E m \in Machines : Book(c, q, m)
  \/ \E m \in Machines : Register(m) \/ AnswerAsk(m) \/ Deliver(m) \/ Apply(m) \/ Restart(m)
  \/ Drop \/ Bounce \/ Lose

Fairness ==
  /\ \A c \in Coords :
       /\ WF_vars(RecvReg(c)) /\ WF_vars(RecvAck(c)) /\ WF_vars(RecvNack(c))
       /\ WF_vars(RecvGone(c))
       /\ \A q \in Requests : WF_vars(Order(c, q)) /\ WF_vars(\E m \in Machines : Book(c, q, m))
  /\ \A m \in Machines : WF_vars(Register(m)) /\ WF_vars(AnswerAsk(m)) /\ WF_vars(Deliver(m))
                         /\ WF_vars(Apply(m))
  /\ WF_vars(Drop) /\ WF_vars(Bounce) /\ WF_vars(Recover)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
\* The properties.

\* An order of an old term never takes effect after the machine wrote a
\* newer term.
NoLateEffect == ~late

\* No role is hosted two times for one request.
OnceEach == \A q \in Requests : count[q] <= 1

\* A role is hosted only on the machine that its booking names: no role
\* is hosted that the journal freed or never booked there.
EffectBooked == \A m \in Machines : \A q \in done[m] : q \in booked /\ bookM[q] = m

\* A new term never loses a hosted role that a machine reported: for each
\* registration of the current incarnation in the holder's view, every role
\* that the machine hosts is in the registration, in an ack of the term, or
\* in an order of the term. An order counts, because the holder sends it
\* again until an ack or a registration shows the role.
Complete ==
  \A m \in Machines :
    LET e == view[holder][m] IN
    (cterm[holder] = term /\ e.ok /\ e.inc = inc[m])
      => done[m] \subseteq e.roles \cup acked[holder] \cup sentQ[holder]

\* Once the steps stop, every request that is not freed is booked, takes
\* effect on its machine, and the holder knows it.
Settled ==
  /\ cterm[holder] = term
  /\ \A q \in Requests \ freed :
       /\ q \in booked /\ q \in done[bookM[q]]
       /\ q \in view[holder][bookM[q]].roles \cup acked[holder]
Settles == <>[]Settled

\* Every sender stops: a deposed coordinator stops ordering.
Quiet == <>[](net = {})
=============================================================================

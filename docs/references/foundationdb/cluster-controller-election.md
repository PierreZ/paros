# foundationdb: Cluster Controller leader election

An analysis of how **FoundationDB elects its Cluster Controller (CC)**, the singleton that recruits
every other role (master/sequencer, commit proxies, tlogs, ...). This is FDB's *leader election*
layer, and it is the cleanest production reference for the thing paros Stage 3 needs: a
**randomized, heartbeat-leased, quorum leader election** that breaks the dueling-proposer symmetry.

Source: FDB at commit `034150211` (local checkout `cpp/foundationdb/foundationdb`). House style:
**(a) verbatim code + `file:line`, then (b) principle, then (c) maps-to-sans-IO**. Paths below are
relative to the FDB source root.

> **Headline:** the CC election is **not Paxos**. It is a *register-based quorum election* over the
> coordinators. Candidates write their identity into every coordinator, each coordinator
> independently nominates the "best" candidate it has seen, and a watcher/candidate computes the
> winner as the candidate a **majority of coordinators** nominate. The replicated *data plane* (the
> transaction log) is a separate mechanism the CC recruits *after* it wins. paros, following the
> etcd-raft model, instead **folds election into the same ballot/Paxos core**, so read this for the
> *election dynamics* (backoff, lease, step-down, uniqueness), not the architecture.

---

## 1. The three tiers

| Tier | Who | Role in election |
|------|-----|------------------|
| **Coordinator** | fixed small set (the cluster file) | holds the volatile leader *register*, nominates a candidate, confirms the leader's heartbeat lease |
| **Candidate** | any `fdbserver` willing to be CC | spams candidacy to all coordinators, declares itself leader on a quorum, then heartbeats |
| **Watcher** | every process and client | polls coordinators to *discover* the leader (same quorum rule) |

Coordinator election state (`availableCandidates`, `availableLeaders`, `currentNominee`) is
**in-memory, not persisted**. Only the separate `GenerationRegInterface` (the on-disk coordinated
state for the transaction system) is durable. Candidates re-register every interval, so a
coordinator restart self-heals within one round (`fdbserver/coordinator/Coordination.cpp:284-559`).

**(c) maps-to-paros:** paros has no coordinator tier. Acceptors *are* the quorum, and the "register"
is the per-node `max_promised_ballot`. The lesson that transfers: leadership can ride on **volatile**
state re-established by continuous candidacy/heartbeat, as long as the *safety* state (promises) is
durable. paros keeps the same split, with `HardState` durable and `Role`/`leader`/election counters
volatile and rebuilt by ticks and messages.

---

## 2. `LeaderInfo.changeID`, the ballot analog

```cpp
// fdbclient/include/fdbclient/CoordinationInterface.h:188-189
UID changeID;
static const uint64_t changeIDMask = ~(uint64_t(0b1111111) << 57);

// :199
bool operator<(LeaderInfo const& r) const { return changeID < r.changeID; }

// :207-210  (top 7 bits = fitness; bottom 57 bits = a RANDOM process id)
void updateChangeID(ClusterControllerPriorityInfo info) {
    changeID = UID(((uint64_t)info.processClassFitness << 57) |
                   ((uint64_t)info.isExcluded        << 60) |
                   ((uint64_t)info.dcFitness         << 61) |
                   (changeID.first() & changeIDMask),
                   changeID.second());
}
```

**(b) principle:** identity is a single comparable UID. The **high 7 bits encode fitness**
(process-class, excluded, DC) and the **low 57 bits are random**
(`deterministicRandom()->randomUniqueID()`, `LeaderElection.actor.cpp:147`). Full-UID `operator<`
means best fitness wins first, and the **random id is the tie-breaker** between equally-fit
candidates. `changeIDMask` strips the fitness bits so a process keeps the *same* identity across
fitness changes.

**(c) maps-to-paros:** `changeID` is analogous to paros `Ballot { round, node }`. FDB's "random low
bits" play the role paros gets from the **driver-drawn randomized election timeout `[T, 2T)`**. Both
exist purely to make two would-be leaders pick different moments/identities and stop dueling. FDB
bakes priority into the ballot (sticky, fitness-based leadership), whereas paros keeps `Ballot` pure
and leaves "who should lead" to timing. Worth noting if paros ever wants preferred leaders.

---

## 3. Candidate side: candidacy, quorum, win

```cpp
// fdbserver/core/LeaderElection.actor.cpp:151-161  - blast candidacy to ALL coordinators
for (int i = 0; i < coordinators.leaderElectionServers.size(); i++)
    cand.push_back(submitCandidacy(coordinators.clusterKey,
                                   coordinators.leaderElectionServers[i],
                                   myInfo, prevChangeID, &nomineeChange, &nominees[i]));

// :196  - the win condition
if (leader.present() && leader.get().second && leader.get().first.equalInternalId(myInfo)) {
    // a quorum of coordinators nominated *me*  => I am leader
    iAmLeader = true; break;
}
```

`getLeader()` is the shared quorum function (used by candidates *and* watchers):

```cpp
// fdbclient/MonitorLeader.cpp:535-577 (condensed)
// mask fitness bits, count votes per process id, winner = most-voted; majority iff:
bool majority = bestCount >= nominees.size() / 2 + 1;
return std::pair<LeaderInfo, bool>(nominees[maskedNominees[bestIdx].second].get(), majority);
```

**(b) principle:** no explicit prepare/accept. A candidate registers, each coordinator nominates its
best-seen candidate, and you are leader iff a **majority of coordinators (`N/2 + 1`) name you**.
Because a majority can only ever converge on one masked id, **at most one leader exists per
agreement**. That is leader *uniqueness*, and it falls straight out of quorum intersection.

**(c) maps-to-paros:** this is paros Phase 1 in disguise. `submitCandidacy` is like broadcasting
`Prepare`, a coordinator's nomination is like a `Promise`, and `getLeader().second` (a promise
quorum) is like paros' `try_become_leader` on a `Promise` quorum. The **`<=1 leader per ballot`
oracle** paros Stage 3 asserts is the same property FDB gets here from majority nomination.

---

## 4. Coordinator side: the register and sticky selection

```cpp
// fdbserver/coordinator/Coordination.cpp:393-406  - register a candidacy
availableCandidates.erase(LeaderInfo(req.prevChangeID));
availableCandidates.insert(req.myInfo);

// :470-472  - pick the nominee each interval
if (availableCandidates.size() &&
    (!availableLeaders.size() ||
     availableLeaders.begin()->leaderChangeRequired(*availableCandidates.begin())))
    nextNominee = *availableCandidates.begin();      // best candidate
else if (availableLeaders.size())
    nextNominee = *availableLeaders.begin();         // keep the stable leader
```

**(b) principle:** the coordinator keeps two ordered sets, `availableCandidates` (registered) and
`availableLeaders` (currently heartbeating), plus `currentNominee`. It only switches away from a live
leader when `leaderChangeRequired` says a candidate is *meaningfully* better. That is **sticky
leadership**: a healthy leader is not unseated by a marginally-fitter newcomer.

**(c) maps-to-paros:** paros stickiness comes from the heartbeat resetting followers'
`election_elapsed` *before* they time out. A live leader simply prevents challengers from ever
starting Phase 1. Same outcome (stable leader), different mechanism (timing vs. explicit comparison).

---

## 5. Anti-dueling: backoff plus bad-candidate timeout (the livelock-fix analog)

```cpp
// fdbserver/coordinator/Coordination.cpp:500-502  - exponential backoff when NO leader exists
setNextInterval(delay(candidateDelay));
candidateDelay = std::min(SERVER_KNOBS->CANDIDATE_MAX_DELAY,
                          candidateDelay * SERVER_KNOBS->CANDIDATE_GROWTH_RATE);

// fdbserver/core/LeaderElection.actor.cpp:211-219 - nominated but can't win => give up, NEW id
if ((!leader.present() || !leader.get().second) &&
    std::find(nominees.begin(), nominees.end(), myInfo) != nominees.end()) {
    if (!badCandidateTimeout.isValid())
        badCandidateTimeout = delay(SERVER_KNOBS->POLLING_FREQUENCY * 2, ...); // 4s
}
// ...on fire: break the loop, regenerate changeID and recompete
```

Defaults (`fdbserver/core/ServerKnobs.cpp:793-797`): `CANDIDATE_MIN_DELAY=0.05s`,
`CANDIDATE_MAX_DELAY=1.0s`, `CANDIDATE_GROWTH_RATE=1.2`, giving `0.05, 0.06, 0.072, ..., 1.0` capped.

**(b) principle:** two layers stop livelock. (1) When there is *no* leader, coordinators **back off
exponentially** between election rounds, so candidates don't thunder. (2) A candidate that is
nominated by some but never reaches a quorum **times out (4s) and regenerates its id**, yielding to a
better-connected peer. The randomized id plus backoff make the symmetry break *probabilistically*.

**(c) maps-to-paros:** this is exactly the Stage 3 dueling-proposer fix. paros' **randomized election
timeout `[T, 2T)`** is FDB's randomized backoff, and `on_nack`/higher-ballot **step-down then wait
for a fresh randomized timeout** is FDB's bad-candidate timeout plus new id. The takeaway: do *not*
immediately re-`Prepare` on rejection. Step down and let randomized timing reschedule you. That is
the single most important behavior to port.

---

## 6. Leadership is a lease, not a term

```cpp
// fdbserver/core/LeaderElection.actor.cpp:269-291 (leader heartbeat loop, condensed)
choose {
    when(wait(quorum(true_heartbeats,  N/2 + 1))) { /* still leader */ }
    when(wait(quorum(false_heartbeats, N/2 + 1))) { break; /* ReplacedAsLeader */ }
    when(wait(delay(SERVER_KNOBS->POLLING_FREQUENCY))) { break; /* ReleasingLeadership */ }
}
// heartbeat every HEARTBEAT_FREQUENCY (0.5s)
```

```cpp
// fdbserver/coordinator/Coordination.cpp:44-62 - coordinator-side liveness lease
class LivenessChecker { void confirmLiveness(){ lastTime.set(now()); } Future<Void> checkStuck() const; };
// armed with COORDINATOR_LEADER_CONNECTION_TIMEOUT = 20s
```

**(b) principle:** the leader renews a **lease** by heartbeating every `0.5s`. It *keeps* leadership
only while a **quorum answers `true`**. It **steps down immediately** if a quorum answers `false`
(someone better won) or if `POLLING_FREQUENCY=2s` passes without a quorum (it can't reach a majority,
so no split brain). Coordinators independently declare the leader dead after `20s` of silence, which
reopens the election.

**(c) maps-to-paros:** maps directly onto Stage 3's `Heartbeat` self-event. A paros leader does
`heartbeat_elapsed >= heartbeat_timeout`, then broadcasts `Heartbeat`, and followers reset
`election_elapsed` on it. The **step-down-on-lost-quorum** rule is the safety-critical part to copy: a
paros leader whose promise was raised by a competing higher `Prepare` must stop streaming `Accept`s
(it can no longer self-accept), mirroring FDB releasing leadership when it loses the quorum.

---

## 7. Discovery is partition-safe by the same quorum rule

```cpp
// fdbclient/MonitorLeader.cpp:495-530 - one watcher actor per coordinator, re-uses getLeader()
// a watcher only commits to a leader when getLeader(nominees).second == true (a majority agrees)
```

**(b) principle:** watchers never trust a single coordinator. They wait for `N/2+1` to name the same
leader. During a partition where coordinators disagree, `getLeader()` returns `majority=false` and
watchers hold their last leader rather than flap.

**(c) maps-to-paros:** the paros analog is that learners only treat a value as chosen on an **accept
quorum** (or a `Commit` derived from one), never on one acceptor's say-so. Same "majority-or-nothing"
discipline.

---

## 8. Linkage to the Cluster Controller role (one paragraph)

Winning `tryBecomeLeader` is what *makes* a process the CC. `ClusterController.actor.cpp:3050-3079`
runs `tryBecomeLeader(coordinators, cci, currentCC, ...)`, waits until it observes *itself* as
`currentCC`, then calls `startRole(Role::CLUSTER_CONTROLLER, ...)` and begins recruiting the master,
commit proxies, and tlogs. Election and the data plane are cleanly separated: the election picks *who
coordinates*, and that winner then stands up the *actual* replicated transaction system. paros
collapses this, since the elected leader *is* the Multi-Paxos proposer that streams Phase 2. That is
simpler for a single replicated log but means election bugs and consensus bugs share a code path
(hence the Stage 3 oracles for both).

---

## 9. Knobs (defaults)

| Knob | Default | Meaning |
|------|---------|---------|
| `CANDIDATE_MIN_DELAY` | `0.05s` | floor of election-round backoff (no leader) |
| `CANDIDATE_MAX_DELAY` | `1.0s` | cap of election-round backoff |
| `CANDIDATE_GROWTH_RATE` | `1.2` | backoff multiplier |
| `POLLING_FREQUENCY` | `2.0s` (`8.0s` long-election) | election interval with a leader; leader step-down timeout; bad-candidate is `2x` |
| `HEARTBEAT_FREQUENCY` | `0.5s` (`1.0s` long-election) | leader lease-renewal interval |
| `COORDINATOR_LEADER_CONNECTION_TIMEOUT` | `20.0s` | coordinator declares leader dead after this silence |

`ServerKnobs.cpp:793-797, 1285`. Note the **ratio**: heartbeat (0.5s) is much smaller than step-down
(2s), which is much smaller than dead (20s), so a healthy leader renews many times before anyone
challenges. That is the same `heartbeat << election` margin paros Stage 3 must keep
(`heartbeat_timeout` much smaller than `T`).

---

## Why this matters for paros

Five patterns to port into the sans-IO core plus driver:

1. **Randomized identity/backoff breaks dueling.** paros draws the election timeout `[T, 2T)` from
   the driver's `RandomProvider`; FDB randomizes the changeID low bits and backs off. Same purpose.
2. **Quorum-to-win implies one leader.** `N/2+1` nomination is paros' `Promise` quorum; it gives the
   `<=1 leader per ballot` invariant for free via quorum intersection.
3. **Heartbeat lease plus step-down on lost quorum.** A `Heartbeat` self-event renews; a leader that
   can't hold the quorum (or whose promise was superseded) must relinquish, not stream blindly.
4. **Don't re-prepare on rejection, step down and wait for a fresh randomized timeout.** FDB's
   bad-candidate timeout is the direct analog of replacing paros' Stage 2 `on_nack` *stall* with a
   timed, randomized retry. This is the livelock fix.
5. **Volatile leadership over durable safety.** Election/lease state is volatile and self-healing;
   only the safety state is persisted. paros keeps `Role`/election counters volatile and `HardState`
   durable.

**The key divergence to remember:** FDB *separates* leader election (volatile register quorum) from
the replicated log (recruited transaction system). paros *unifies* them in the Ballot/Paxos core
(the etcd-raft model). So FDB is a reference for the **election dynamics**, not the layering.

See also: [`../papers/paxos-vs-raft/`](../papers/paxos-vs-raft/) (Paxos and Raft differ *only* in
leader election) and the sans-IO core model
[`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md)
(tick-driven election, `MsgHup`/heartbeat self-events, the architecture paros follows).

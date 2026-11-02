//! The cell coordinator's failure detector over the cell's machines (#211): [`Watch`] and the `Identify` probes of a machine's new address.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use moonpool_core::{Detach, LocalTaskProvider};
use moonpool_rpc::RpcHandle;
use paros_client::cell::CellSession;
use paros_client::checkpoint::Folder;
use paros_client::fleet::Interrupted;
use paros_client::{Client, bootstrap};
use paros_core::NodeId;
use paros_model::address::{Address, Names};
use paros_model::fleet::FleetDirectory;
use paros_model::machine::incarnation_from_halves;
use paros_model::registry::{MaintenanceScope, NodeStanding, Registry, SystemCommand};
use paros_net::runtime::ParosProviders;
use paros_wire::machine as wire;

use crate::cell::slow::{Sample, SlowBounds, SlowJudge};

/// The cell coordinator's failure detector over the cell's machines (#211,
/// D6): when each machine last answered `Identify`, and when its next round
/// is due. It writes only changes into the cell control journal, through
/// the term's session.
#[derive(Debug)]
pub struct Watch {
    /// How long a machine is silent before it is marked down.
    pub(crate) down_after: Duration,
    /// How long a machine is silent before its roles are re-placed (#475).
    pub(crate) replace_after: Duration,
    /// When this watch first folded each open maintenance window, keyed by
    /// what it covers, with the position of the entry that opened it
    /// (#359): the window runs on this watch's clock from then.
    pub(crate) windows: BTreeMap<MaintenanceScope, (u64, Duration)>,
    /// The machines this watch named gone: they stay gone while a slot is
    /// booked on them and the registry still holds them down, so a watch
    /// that hears one again does not undo a move before the registry
    /// records it up. Once it is up, it is bookable again: a slot a move
    /// left outside the configuration in force is the mode change's
    /// (`crate::tenant::modes`), as it is for a later term, and a journal a move
    /// shrank grows back onto it. A slot no move releases (a deleted
    /// journal's, a tenant's) must not keep it gone for the term.
    pub(crate) departed: std::collections::BTreeSet<NodeId>,
    /// How often a round runs.
    pub(crate) every: Duration,
    /// When the watch started: a machine never heard is silent since then.
    pub(crate) started: Duration,
    /// When the next round is due.
    pub(crate) next: Duration,
    /// When this watch last watched: the end of its last round, or its
    /// start.
    pub(crate) seen: Duration,
    /// When each machine last answered as itself.
    pub(crate) heard: BTreeMap<NodeId, Duration>,
    /// When this watch first asked each machine: one never heard is silent
    /// since then. A machine the cell's book names only after the watch
    /// started was once judged silent since the start: a machine whose
    /// registration made it up 12 s into a term was marked down at its
    /// first unanswered `Identify` (replay-recipe 12913196830458982579
    /// 651425:1084719881269859971 on a079991 (#710 (recycled segments)),
    /// red on "registry: the cell coordinator never marks a live machine
    /// down in the tail").
    pub(crate) asked: BTreeMap<NodeId, Duration>,
    /// How many asks in a row each machine left unanswered since it last
    /// answered. One missed probe never marks a machine down (#211): a
    /// round lasts longer than `every`, so the time since the last answer
    /// alone passes `down_after` at the first miss when `down_after` is
    /// near `every` (replay-main 17838952018056042006 on 7863404, PR #770
    /// (batcher follow-ups): a machine's connection dropped once as it
    /// joined the cell, 1.4 s after its last answer, past a `down_after` of
    /// 1.1 s, red on "registry: the cell coordinator never marks a live
    /// machine down in the tail").
    pub(crate) missed: BTreeMap<NodeId, u32>,
    /// Whether the last call ran a round.
    pub(crate) ran: bool,
    /// The `Identify` checks a `Register` left running, by machine (#454
    /// (control plane meets chaos)): see [`register`].
    pub(crate) probes: BTreeMap<NodeId, Probe>,
    /// The registry fold position each admitted machine reported at its
    /// last answer (#505 (stranded admitted machine)).
    pub(crate) positions: BTreeMap<NodeId, u64>,
    /// The universe directory's fold, read at each round: the cell's join
    /// (#513 (fleet Init duel)) and the tenant operations in flight (#225).
    pub(crate) directory: Folder<FleetDirectory>,
    /// The slow-machine judgement (#361 (slow machines)).
    pub(crate) slow: SlowJudge,
    /// Each machine's busyness step from its last `Load` window (#837
    /// (remove role slots)), as the last round measured it: placement
    /// books the least busy machine of a zone first. A machine that did not
    /// answer is left out, so it counts as idle.
    pub(crate) busy: BTreeMap<NodeId, u8>,
    /// The journals each machine leads, from its last `Load` (#687
    /// (rebalance a cell)): the balancer evens them out.
    pub(crate) leading: paros_model::balance::Leading,
    /// The free-bytes floor (#360 (disk full)): a machine that answers
    /// `Load` with less free space is marked low on space.
    pub(crate) space_floor: u64,
}

/// One `Identify` check of a machine's new address, run off the renewal
/// path with the client's request timeout. `answered` is `None` while it
/// runs, then whether the machine answered there as that incarnation.
#[derive(Debug)]
pub(crate) struct Probe {
    pub(crate) addr: Address,
    pub(crate) incarnation: u128,
    pub(crate) answered: Arc<std::sync::Mutex<Option<bool>>>,
}

impl Watch {
    /// A watch started `now`, running a round `every` period, marking a
    /// machine down after `down_after` of silence, naming it gone after
    /// `replace_after` (#475), and slow by `slow` (#361 (slow machines)).
    ///
    /// # Panics
    ///
    /// If `down_after` is not above `every`: one missed round would mark a
    /// machine down (the tunables' floor refuses it). If `replace_after` is
    /// not above `down_after`: a machine would be re-placed before it is
    /// down (the tunables' floor refuses it too).
    #[must_use]
    pub fn new(
        now: Duration,
        every: Duration,
        (down_after, replace_after): (Duration, Duration),
        slow: SlowBounds,
    ) -> Self {
        assert!(!every.is_zero(), "a watch round has a period");
        assert!(
            down_after > every,
            "one missed round never marks a machine down"
        );
        assert!(
            replace_after > down_after,
            "a machine is re-placed only once it is down"
        );
        Self {
            down_after,
            replace_after,
            windows: BTreeMap::new(),
            departed: std::collections::BTreeSet::new(),
            every,
            started: now,
            next: now,
            seen: now,
            heard: BTreeMap::new(),
            asked: BTreeMap::new(),
            missed: BTreeMap::new(),
            ran: false,
            probes: BTreeMap::new(),
            positions: BTreeMap::new(),
            directory: Folder::new(FleetDirectory::default()),
            slow: SlowJudge::new(slow),
            busy: BTreeMap::new(),
            leading: BTreeMap::new(),
            space_floor: 0,
        }
    }

    /// The same watch, marking a machine low on space below `floor` free
    /// bytes (#360 (disk full)); `0` marks none.
    #[must_use]
    pub fn with_space_floor(mut self, floor: u64) -> Self {
        self.space_floor = floor;
        self
    }

    /// Whether the last [`Watch::round`] call ran a round.
    #[must_use]
    pub fn ran(&self) -> bool {
        self.ran
    }

    /// This watch resumed at `now` by a later term of the same candidacy:
    /// it keeps what it observed, and every moment it holds moves forward
    /// by the time since it last watched, so a machine's silence counts
    /// only the time a round could have heard it. The next round is due at
    /// once.
    ///
    /// # Panics
    ///
    /// If `now` is before the watch last watched.
    #[must_use]
    pub fn resumed(mut self, now: Duration) -> Self {
        assert!(now >= self.seen, "a watch resumes after it last watched");
        let gap = now.saturating_sub(self.seen);
        if !self.heard.is_empty() {
            moonpool_assertions::reachable!("coordinator: a term resumed the last term's watch");
        }
        self.started += gap;
        for since in self.heard.values_mut().chain(self.asked.values_mut()) {
            *since += gap;
        }
        for (_, seen) in self.windows.values_mut() {
            *seen += gap;
        }
        self.seen = now;
        self.next = now;
        self.ran = false;
        assert!(self.started <= now, "a resumed watch started by now");
        assert!(
            self.heard.values().all(|since| *since <= now),
            "a resumed watch heard every machine by now"
        );
        assert!(
            self.asked.values().all(|since| *since <= now),
            "a resumed watch asked every machine by now"
        );
        self
    }

    /// Since when machine `id` is silent: its last answer, else this
    /// watch's first ask of it, else the watch's start.
    fn silent_since(&self, id: NodeId) -> Duration {
        let since = self
            .heard
            .get(&id)
            .or_else(|| self.asked.get(&id))
            .copied()
            .unwrap_or(self.started);
        assert!(
            since >= self.started,
            "a machine is silent since the watch watched"
        );
        since
    }

    /// The machines whose roles the term re-places at `now` (#475 (re-place
    /// after the Down bound)): registered machines `registry` holds down
    /// that this watch has not heard for `replace_after`, outside every open
    /// maintenance window over them or their zone (#359), and every machine
    /// named gone before that still holds a slot, is still down, and no open
    /// window holds. A
    /// machine that answers again inside the bound keeps its roles: a
    /// reboot rejoins in place.
    ///
    /// # Panics
    ///
    /// A machine named gone is not in `registry`: a watch bug.
    pub fn gone(
        &mut self,
        registry: &Registry,
        now: Duration,
    ) -> std::collections::BTreeSet<NodeId> {
        // Each window runs from when this watch first folded the entry that
        // opened it: the registry holds no time.
        let open: BTreeMap<MaintenanceScope, u64> = registry
            .maintenance()
            .map(|(scope, window)| (scope.clone(), window.seq))
            .collect();
        // A window an operator replaced (a new entry over the same scope)
        // runs from when this watch folds the new entry (#661 (maintenance
        // windows)).
        if self
            .windows
            .iter()
            .any(|(scope, (seq, _))| open.get(scope).is_some_and(|now| now != seq))
        {
            moonpool_assertions::reachable!(
                "coordinator: a replaced maintenance window restarts its clock"
            );
        }
        self.windows
            .retain(|scope, (seq, _)| open.get(scope) == Some(seq));
        for (scope, seq) in open {
            self.windows.entry(scope).or_insert((seq, now));
        }
        self.seen = self.seen.max(now);
        self.departed
            .retain(|node| registry.booked(*node) > 0 && !registry.liveness(*node).up);
        // A window the operator opened over a machine named gone before
        // holds its roles all the same: the coordinator re-places no role
        // of a down machine a window covers (#359). Without this, a machine
        // gone, back up, then down again stayed gone through its window, and
        // a set released its slot there (sweep recipe `354564601073116033
        // 651466:18426518659794956008,189780:8849847318170525142` on
        // deadab0, PR #710 (adopt recycled segments)).
        let mut gone: std::collections::BTreeSet<NodeId> = self
            .departed
            .iter()
            .copied()
            .filter(|id| !self.held_one(registry, *id, now))
            .collect();
        for (id, node) in registry.nodes() {
            if node.standing != NodeStanding::Registered
                || registry.liveness(id).up
                || gone.contains(&id)
            {
                continue;
            }
            let since = self.silent_since(id);
            if now.saturating_sub(since) < self.replace_after {
                continue;
            }
            let covered = registry.maintenance_of(id).next().is_some();
            let held = self.held_one(registry, id, now);
            if held {
                if registry.booked(id) > 0 {
                    moonpool_assertions::reachable!(
                        "coordinator: a maintenance window holds a down machine's roles"
                    );
                }
                continue;
            }
            if covered && registry.booked(id) > 0 {
                moonpool_assertions::reachable!(
                    "coordinator: a maintenance window ended while its machine was down"
                );
            }
            if registry.booked(id) > 0 {
                moonpool_assertions::reachable!(
                    "coordinator: a machine down past its bound is gone"
                );
                tracing::info!(machine = id.0, "machine_gone");
            }
            gone.insert(id);
        }
        self.departed
            .extend(gone.iter().copied().filter(|n| registry.booked(*n) > 0));
        for id in &gone {
            assert!(
                registry.get(*id).is_some(),
                "a machine named gone is registered"
            );
        }
        gone
    }

    /// Whether an open maintenance window over `id` or its zone holds its
    /// roles at `now` (#359), on this watch's clock.
    fn held_one(&self, registry: &Registry, id: NodeId, now: Duration) -> bool {
        registry.maintenance_of(id).any(|(scope, window)| {
            self.windows
                .get(scope)
                .is_some_and(|(_, seen)| now < *seen + Duration::from_millis(window.for_ms))
        })
    }

    /// The registered machines an open maintenance window holds at `now`
    /// (#359): a slow mark moves none of their roles (#361 (slow
    /// machines)). Read after [`Watch::gone`], which opens the windows.
    ///
    /// # Panics
    ///
    /// If it names a machine the registry does not hold.
    #[must_use]
    pub fn held(&self, registry: &Registry, now: Duration) -> std::collections::BTreeSet<NodeId> {
        let held: std::collections::BTreeSet<NodeId> = registry
            .nodes()
            .filter(|(id, node)| {
                node.standing == NodeStanding::Registered && self.held_one(registry, *id, now)
            })
            .map(|(id, _)| id)
            .collect();
        assert!(
            held.iter().all(|id| registry.get(*id).is_some()),
            "a held machine is registered"
        );
        held
    }

    /// Run one round if one is due at `now`, writing each change through
    /// `session`: how many entries it wrote.
    ///
    /// # Panics
    ///
    /// If its own bookkeeping is broken: a machine that answered still
    /// counts a missed ask, or one was heard after the round's end.
    ///
    /// # Errors
    ///
    /// A write did not land: the term writes nothing more.
    #[tracing::instrument(level = "trace", skip_all, fields(cell = session.registry().fleet().map_or(0, |f| f.cell_id)))]
    pub async fn round<P: ParosProviders>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        client: &Client<P>,
        names: &Names,
        session: &mut CellSession,
    ) -> Result<usize, Interrupted> {
        self.ran = client.now() >= self.next;
        if !self.ran {
            return Ok(0);
        }
        self.next = client.now() + self.every;
        let timeout = client.tunables().request_timeout;
        // Each machine where the cell's address book says it is (#349).
        let machines: BTreeMap<NodeId, Address> =
            paros_model::machine::cell_book(session.founders(), session.registry())
                .into_iter()
                .collect();
        // The slots of the tenants the cell dropped go back to it (#212
        // (placement)): no request for such a tenant is answered again, so
        // nothing else would release them.
        let dropped: std::collections::BTreeSet<_> = session
            .registry()
            .bookings()
            .map(|(_, b)| b.target.tenant())
            .filter(|tenant| session.registry().dropped(*tenant))
            .collect();
        let mut wrote = 0;
        // The cell's side of an `init` (#513 (fleet Init duel)) and of each
        // tenant operation in flight (#225): the operator waits for it, and
        // claims nothing here.
        wrote += crate::universe::follow(client, session, &mut self.directory).await?;
        if !dropped.is_empty() {
            wrote +=
                crate::tenant::release(client, session, |b| dropped.contains(&b.target.tenant()))
                    .await?;
            moonpool_assertions::reachable!("coordinator: a dropped tenant's slots are released");
        }
        let watched: Vec<NodeId> = machines
            .keys()
            .copied()
            .filter(|id| session.registry().contains(*id))
            .collect();
        let mut answered: Vec<(NodeId, Address, Duration)> = Vec::new();
        for (id, addr) in machines {
            let asked = client.now();
            self.asked.entry(id).or_insert(asked);
            let answer = bootstrap::identify(providers, rpc, names, &addr, timeout)
                .await
                .filter(|ack| ack.node_id == id.0);
            if answer.is_some() {
                answered.push((id, addr.clone(), client.now().saturating_sub(asked)));
            }
            // Asked now, answered or not: a moment this watch watched.
            self.seen = client.now();
            let command = if let Some(ack) = &answer {
                self.heard.insert(id, client.now());
                self.missed.remove(&id);
                Self::answered(session, id, &addr, ack)
            } else {
                *self.missed.entry(id).or_insert(0) += 1;
                self.silent(session, id, client.now())
            };
            assert!(
                answer.is_none() || !self.missed.contains_key(&id),
                "a machine that answered missed no ask"
            );
            if let Some(command) = command {
                session.record(client, 0, &command).await?;
                wrote += 1;
            }
            // A machine whose registry fold is stuck gets the cell's book
            // again (#505 (stranded admitted machine)).
            if let Some(ack) = answer
                && self.stuck(session, id, &ack)
                && session
                    .readmit(providers, rpc, names, (id, &addr), timeout)
                    .await
            {
                moonpool_assertions::reachable!(
                    "coordinator: a stuck machine is handed the cell's book again"
                );
                tracing::info!(
                    machine = id.0,
                    position = session.position(),
                    "machine_readmitted"
                );
            }
        }
        wrote += self
            .judge_slow(
                providers,
                rpc,
                names,
                client,
                session,
                (&answered, &watched),
            )
            .await?;
        self.seen = client.now();
        assert!(
            self.heard.values().all(|since| *since <= self.seen),
            "a watch heard every machine by its last round"
        );
        Ok(wrote)
    }

    /// The slow-machine half of a round (#361 (slow machines)): ask every
    /// machine that answered `Identify` for its `Load`, judge the round's
    /// facts, and write each change of judgement `MachineSlow`, for the
    /// incarnation the registry holds. How many entries it wrote.
    async fn judge_slow<P: ParosProviders>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        names: &Names,
        client: &Client<P>,
        session: &mut CellSession,
        (answered, watched): (&[(NodeId, Address, Duration)], &[NodeId]),
    ) -> Result<usize, Interrupted> {
        // Only a machine of the pool is judged: the registry refuses a mark
        // of any other.
        let judged: Vec<&(NodeId, Address, Duration)> = answered
            .iter()
            .filter(|(id, _, _)| watched.contains(id))
            .collect();
        let asks: Vec<(u64, Address)> = judged
            .iter()
            .map(|(id, addr, _)| (id.0, addr.clone()))
            .collect();
        let loads = paros_client::load::ask_all(
            providers,
            rpc,
            names,
            &asks,
            client.tunables().request_timeout,
        )
        .await;
        let samples: BTreeMap<NodeId, Sample> = judged
            .iter()
            .map(|(id, _, rtt)| {
                let ack = match loads.get(&id.0) {
                    Some(paros_client::load::LoadOutcome::Answered(ack)) => Some(&**ack),
                    _ => None,
                };
                (*id, Sample::of(*rtt, ack))
            })
            .collect();
        self.leading = judged
            .iter()
            .filter_map(|(id, _, _)| match loads.get(&id.0) {
                Some(paros_client::load::LoadOutcome::Answered(ack)) => Some((
                    *id,
                    ack.leading
                        .iter()
                        .map(|led| {
                            paros_core::journal::JournalIdentifier::new(
                                paros_core::journal::TenantId(led.tenant),
                                paros_core::journal::JournalId(led.journal),
                            )
                        })
                        .collect(),
                )),
                _ => None,
            })
            .collect();
        self.busy = samples
            .iter()
            .map(|(id, sample)| {
                let step = paros_model::placement::busy_step(
                    sample.cpu_busy_permille,
                    sample.disk_busy_permille,
                );
                (*id, step)
            })
            .collect();
        if self.busy.values().any(|step| *step > 0) {
            moonpool_assertions::reachable!("coordinator: a round measures a busy machine");
        }
        let registry = session.registry();
        let changes = self
            .slow
            .round(&samples, |id| registry.is_slow(id), watched);
        let mut wrote = 0;
        for change in changes {
            let held = session.registry().liveness(change.id);
            moonpool_assertions::reachable!("coordinator: a machine's slow judgement changes");
            if change.slow {
                moonpool_assertions::reachable!("coordinator: a slow machine is marked slow");
            } else {
                moonpool_assertions::reachable!("coordinator: a slow machine is fast again");
            }
            tracing::info!(
                machine = change.id.0,
                slow = change.slow,
                rtt_us = change.facts.rtt_us,
                disk = change.facts.disk_busy_permille,
                cpu = change.facts.cpu_busy_permille,
                "machine_slow"
            );
            let command = SystemCommand::MachineSlow {
                id: change.id,
                incarnation: held.incarnation,
                slow: change.slow,
                facts: change.facts,
            };
            session.record(client, 0, &command).await?;
            wrote += 1;
        }
        wrote += self.judge_space(client, session, &judged, &loads).await?;
        Ok(wrote)
    }

    /// The space half of a round (#360 (disk full)): every machine whose
    /// `Load` answer reports its disk's space is low on space below the
    /// free-bytes floor, and each change of judgement is written
    /// `MachineSpace`, for the incarnation the registry holds. How many
    /// entries it wrote.
    async fn judge_space<P: ParosProviders>(
        &self,
        client: &Client<P>,
        session: &mut CellSession,
        judged: &[&(NodeId, Address, Duration)],
        loads: &BTreeMap<u64, paros_client::load::LoadOutcome>,
    ) -> Result<usize, Interrupted> {
        let mut wrote = 0;
        for (id, _, _) in judged {
            let Some(paros_client::load::LoadOutcome::Answered(ack)) = loads.get(&id.0) else {
                continue;
            };
            if !ack.has_space {
                continue;
            }
            let low = ack.free_bytes < self.space_floor;
            let held = session.registry().liveness(*id);
            if held.low_space == low {
                continue;
            }
            if low {
                moonpool_assertions::reachable!("coordinator: a machine is marked low on space");
            } else {
                moonpool_assertions::reachable!(
                    "coordinator: a machine low on space has space again"
                );
            }
            tracing::info!(
                machine = id.0,
                low,
                free_bytes = ack.free_bytes,
                "machine_space"
            );
            let command = SystemCommand::MachineSpace {
                id: *id,
                incarnation: held.incarnation,
                low,
                free_bytes: ack.free_bytes,
            };
            session.record(client, 0, &command).await?;
            wrote += 1;
        }
        Ok(wrote)
    }

    /// Whether the admitted machine `id`, answering `ack`, folds the
    /// registry no more (#505 (stranded admitted machine)): it serves the
    /// cell, and it reports the same fold position below the registry's
    /// tail as at its last answer. Such a machine dials its cell through a
    /// book whose every founding member moved or was wiped, and nothing
    /// booked on it can start until it learns where they are.
    fn stuck(&mut self, session: &CellSession, id: NodeId, ack: &wire::IdentifyAck) -> bool {
        let position = ack.registry_position;
        let before = self.positions.insert(id, position);
        // A founding member folds its own copy of the registry: it reports
        // no position, and is never admitted.
        if session.members().contains(&id) {
            return false;
        }
        let serving = session.registry().serves(id) && ack.cell_id == session.journals().cell_id;
        serving && position < session.position() && before == Some(position)
    }

    /// The change machine `id` answering `ack` at `addr` makes, if any: a
    /// reboot registers again, a registered machine seen in the cell is
    /// admitted, then liveness.
    fn answered(
        session: &CellSession,
        id: NodeId,
        addr: &Address,
        ack: &paros_wire::machine::IdentifyAck,
    ) -> Option<SystemCommand> {
        let incarnation = incarnation_from_halves(ack.incarnation_high, ack.incarnation_low);
        if incarnation == 0 {
            return None;
        }
        let registry = session.registry();
        let cell_id = session.journals().cell_id;
        let held = registry.liveness(id);
        if !held.up && registry.booked(id) > 0 {
            // A machine back before its roles moved keeps them: a reboot
            // rejoins in place (#475).
            moonpool_assertions::reachable!("coordinator: a machine marked down rejoins in place");
        }
        match registry.get(id) {
            Some(node) if node.incarnation != incarnation => {
                // A reboot of a registered machine: it registers again, as
                // the incarnation that answered (up, by its registration).
                let (class, locality) = paros_model::machine::identity_tags(ack)?;
                // A reboot may widen its roles to `all`, never narrow them:
                // the registry refuses fewer roles than it holds.
                if class != node.class && class != paros_model::machine::Class::All {
                    return None;
                }
                moonpool_assertions::reachable!("coordinator: a rebooted machine registers again");
                Some(SystemCommand::RegisterNode {
                    id,
                    addr: addr.to_string(),
                    class,
                    locality,
                    name: ack.name.clone(),
                    incarnation,
                    machine_interface: ack.machine_interface.clone(),
                })
            }
            // A registered machine that answers as a machine of this cell
            // won its `Admit`: placement may book it from now on (#511
            // (placement books machines that never served)). One idle, or
            // stuck in another cell decree, never serves.
            Some(_) if !registry.serves(id) && cell_id != 0 && ack.cell_id == cell_id => {
                moonpool_assertions::reachable!("coordinator: a machine's admission is recorded");
                Some(SystemCommand::MachineAdmitted { id })
            }
            _ if !held.up || held.incarnation != incarnation => {
                if !held.up {
                    moonpool_assertions::reachable!(
                        "coordinator: a machine marked down answers again"
                    );
                }
                Some(SystemCommand::MachineUp { id, incarnation })
            }
            _ => None,
        }
    }

    /// The change machine `id`'s silence at `now` makes, if any: `Down`
    /// once it was silent for `down_after` and left more than one ask in a
    /// row unanswered, unless it is held down.
    fn silent(&self, session: &CellSession, id: NodeId, now: Duration) -> Option<SystemCommand> {
        let held = session.registry().liveness(id);
        let since = self.silent_since(id);
        let missed = self.missed.get(&id).copied().unwrap_or(0);
        assert!(missed > 0, "a silent machine missed this ask");
        if !held.up || now.saturating_sub(since) < self.down_after {
            return None;
        }
        if missed < 2 {
            return None;
        }
        assert!(
            session.registry().seen_alive(id),
            "a machine marked down was seen alive"
        );
        moonpool_assertions::reachable!("coordinator: a silent machine is marked down");
        Some(SystemCommand::MachineDown {
            id,
            incarnation: held.incarnation,
        })
    }
}

/// Whether `answer` is `id`'s incarnation `incarnation` answering
/// `Identify` (see [`register`]).
pub(crate) fn identified(answer: Option<wire::IdentifyAck>, id: NodeId, incarnation: u128) -> bool {
    answer.is_some_and(|ack| {
        ack.node_id == id.0
            && incarnation_from_halves(ack.incarnation_high, ack.incarnation_low) == incarnation
    })
}

/// Start an `Identify` of `addr` that waits up to `timeout`, off the
/// renewal path (see [`register`]).
pub(crate) fn spawn_probe<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    addr: &Address,
    (id, incarnation): (NodeId, u128),
    timeout: Duration,
) -> Probe {
    let answered = Arc::new(std::sync::Mutex::new(None));
    let (slot, task_providers, rpc, names, target) = (
        answered.clone(),
        providers.clone(),
        rpc.clone(),
        names.clone(),
        addr.clone(),
    );
    providers
        .local_task()
        .spawn_local_task("paros-register-probe", async move {
            let answer = bootstrap::identify(&task_providers, &rpc, &names, &target, timeout).await;
            *slot.lock().unwrap_or_else(PoisonError::into_inner) =
                Some(identified(answer, id, incarnation));
        })
        .detach();
    Probe {
        addr: addr.clone(),
        incarnation,
        answered,
    }
}

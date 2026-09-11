//! One matchmaker process: the role, the registry disk it reboots from, and
//! the **step, persist, reply, advance** order every delivery follows.
//!
//! This is `crates/paros-core/examples/matchmaker.rs`'s `MatchmakerNode`: a
//! [`Matchmaker`] role, the static [`MatchmakerConfig`] it boots with, and a
//! [`MemRegistry`] disk it writes to and reboots from. A reply never leaves
//! ahead of an unsynced write, and every batch is acknowledged with
//! [`paros_core::MatchmakerReady::advance`] — a batch that is dropped instead
//! stays pending, and the next one applies its writes a second time.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, GcAck, GcOutcome, GcRequest, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    MatchmakerHardState, MatchmakerId, MemRegistry, ReconfigureReply, ReconfigureRequest,
    Registration,
};

/// One matchmaker's disk: the library's own [`MemRegistry`], plus the two
/// counters that make the flush order visible.
///
/// The **write** half is [`MemRegistry::apply`]; the **read** half is the
/// core's recovery port, so a reboot is `Matchmaker::new(&config, &store)` and
/// nothing else — which is what makes a registration survive a crash here for
/// the same reason it survives one in production.
#[derive(Clone, Debug, Default)]
pub struct RegistryDisk {
    store: MemRegistry,
    /// Writes applied so far.
    writes: usize,
    /// Writes an fsync covers. A reply leaves only while this equals `writes`.
    synced: usize,
}

impl RegistryDisk {
    /// The durable registry.
    #[must_use]
    pub fn store(&self) -> &MemRegistry {
        &self.store
    }

    /// The durable scalars: the watermark, the generation, the phase, the
    /// decree record.
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        self.store.hard_state()
    }

    /// The registry records, in ballot order.
    #[must_use]
    pub fn registrations(&self) -> &BTreeMap<Ballot, Registration> {
        self.store.registrations()
    }

    /// How many durable writes this disk has taken. One acknowledged batch
    /// moves it once; a batch that is never advanced moves it a second time at
    /// the next delivery, which is what this counter is here to catch.
    #[must_use]
    pub fn writes(&self) -> usize {
        self.writes
    }

    /// The fsync. Memory has nothing to flush, so this only records that every
    /// write so far is covered; what matters is **where** it is called.
    fn sync(&mut self) {
        self.synced = self.writes;
    }
}

/// One matchmaker process: the role, the configuration it boots with, and the
/// disk it reboots from. `None` for the role is a crashed matchmaker — its
/// disk survives, exactly as a node's does.
pub struct MatchmakerProcess {
    config: MatchmakerConfig,
    role: Option<Matchmaker>,
    disk: RegistryDisk,
}

impl MatchmakerProcess {
    /// A fresh matchmaker with an empty registry.
    #[must_use]
    pub fn new(id: MatchmakerId, bootstrap: Vec<MatchmakerId>) -> Self {
        Self::seeded(id, bootstrap, BTreeMap::new())
    }

    /// A matchmaker whose registry a level pre-seeded — how a level puts an
    /// earlier leader's registration in place before the player's first move.
    #[must_use]
    pub fn seeded(
        id: MatchmakerId,
        bootstrap: Vec<MatchmakerId>,
        registrations: BTreeMap<Ballot, Registration>,
    ) -> Self {
        let config = MatchmakerConfig { id, bootstrap };
        let disk = RegistryDisk {
            store: MemRegistry::new(MatchmakerHardState::default(), registrations),
            writes: 0,
            synced: 0,
        };
        Self {
            role: Some(Matchmaker::new(&config, disk.store())),
            config,
            disk,
        }
    }

    /// This matchmaker's id.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.config.id
    }

    /// Whether it is running.
    #[must_use]
    pub fn alive(&self) -> bool {
        self.role.is_some()
    }

    /// The live role, if it is running.
    #[must_use]
    pub fn role(&self) -> Option<&Matchmaker> {
        self.role.as_ref()
    }

    /// Its disk.
    #[must_use]
    pub fn disk(&self) -> &RegistryDisk {
        &self.disk
    }

    /// Drop the volatile role; the registry survives.
    pub(super) fn crash(&mut self) {
        self.role = None;
    }

    /// Rebuild the role from the disk, through the core's recovery port.
    pub(super) fn reboot(&mut self) {
        self.role = Some(Matchmaker::new(&self.config, self.disk.store()));
    }

    /// Persist one batch: apply every write, fsync, and only then let the
    /// batch's replies escape.
    ///
    /// # Panics
    ///
    /// If a reply would leave ahead of an unsynced write.
    fn persist(&mut self) {
        let Some(role) = self.role.as_mut() else {
            return;
        };
        let ready = role.ready();
        for op in ready.writes() {
            self.disk.store.apply(op);
            self.disk.writes += 1;
        }
        drop(ready);
        self.disk.sync();
        assert!(
            self.disk.synced == self.disk.writes,
            "no reply leaves ahead of an unsynced write"
        );
    }

    /// Deliver one matchmaking request: step, persist, reply, advance.
    pub(super) fn deliver_match(&mut self, request: MatchRequest) -> Option<MatchReply> {
        self.role.as_mut()?.step(request);
        self.persist();
        let role = self.role.as_mut()?;
        let ready = role.ready();
        let reply = ready.replies().first().cloned();
        ready.advance();
        reply
    }

    /// Deliver one garbage-collection request: raise the floor, persist, ack,
    /// advance.
    ///
    /// The ack is built inside the same `ready()` / `advance()` round the two
    /// deliveries above use. The ack itself carries no bucket of the batch,
    /// but the batch must still be acknowledged: a `MatchmakerReady` that is
    /// dropped instead leaves the raise pending, and the next batch applies it
    /// to the disk a second time.
    pub(super) fn deliver_gc(&mut self, request: GcRequest) -> Option<GcAck> {
        let outcome = self
            .role
            .as_mut()?
            .advance_gc_watermark(request.generation, request.watermark);
        self.persist();
        let role = self.role.as_mut()?;
        let watermark = role.hard_state().gc_watermark;
        role.ready().advance();
        Some(GcAck {
            matchmaker: self.config.id,
            generation: request.generation,
            applied: outcome != GcOutcome::Refused,
            watermark,
        })
    }

    /// Deliver one handover step: step, persist, reply, advance.
    pub(super) fn deliver_reconfigure(
        &mut self,
        request: ReconfigureRequest,
    ) -> Option<ReconfigureReply> {
        self.role.as_mut()?.step_reconfigure(request);
        self.persist();
        let role = self.role.as_mut()?;
        let ready = role.ready();
        let reply = ready.reconfigure_replies().first().cloned();
        ready.advance();
        reply
    }
}

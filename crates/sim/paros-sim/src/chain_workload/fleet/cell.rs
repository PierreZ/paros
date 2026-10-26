//! The cell an operator works on (#246): `init` whole against the machines
//! (`paros::client::initialize`, the code `parosctl init` prints), and the
//! cell learned from the run that formed it or through `Inspect` at the
//! machines — never from the harness (§3.8: no identifier is fixed).

use paros::model::address::Address;
use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::{Providers as _, TimeProvider as _};
use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::client::bootstrap::{self, InitOutcome};
use paros::client::fleet::{FleetRefusal, Stage};
use paros::client::initialize::{InitParams, InitRefusal, InitRun, Initialized};
use paros::core::JournalIdentifier;
use paros::machine::ControlJournals;

use super::super::system::Announce;
use super::{FleetOps, reach};
use crate::client::ChainClient;

/// The cell as this operator learned it (#246): from the `init` it ran, or
/// through `Inspect` at the machines.
#[derive(Clone)]
pub(super) struct Cell {
    /// Its control journals; the fleet tenant's is always named.
    pub(super) journals: ControlJournals,
    /// The fleet tenant's control journal.
    pub(super) fleet: JournalIdentifier,
    /// Its members by id, in id order: the cell control journal's genesis.
    pub(super) members: Vec<u64>,
    /// The members this operator reached, with their addresses: the
    /// servers a fleet step talks to.
    pub(super) servers: Vec<(u64, Address)>,
    /// A client over `servers`, announcing every call to the audit.
    pub(super) client: ChainClient,
}

impl Cell {
    /// The server a call starts at.
    pub(super) fn first(&self, draw: u64) -> usize {
        usize::try_from(draw % self.servers.len().max(1) as u64).unwrap_or(0)
    }

    /// The process IP of the machine a server index names, as the topology
    /// names it (#257: not its advertised address).
    pub(super) fn ip(&self, ctx: &SimContext, server: usize) -> Option<String> {
        self.servers
            .get(server)
            .and_then(|(_, addr)| crate::machine::process_ip(ctx.state(), addr))
    }
}

impl FleetOps {
    /// A client over `servers`, announcing every call at the cell's
    /// journals to the audit and their shared history.
    pub(super) fn connect(&self, ctx: &SimContext, servers: &[(u64, Address)]) -> ChainClient {
        self.connector
            .client(servers)
            .with_observer(Arc::new(
                Announce::every(ctx).learned_only(self.learned.clone()),
            ))
            .rotating_over(servers.len())
    }

    /// A client of the batchers on `servers` (#145 (batchers)), announcing
    /// every call as [`FleetOps::connect`]'s does.
    pub(super) fn connect_batched(&self, servers: &[(u64, Address)]) -> ChainClient {
        let observer = Announce::of_machine(
            &self.state,
            moonpool_sim::Providers::time(self.connector.providers()).clone(),
            self.connector.client_id(),
        )
        .learned_only(self.learned.clone());
        self.connector
            .batched(servers)
            .with_observer(Arc::new(observer))
            .rotating_over(servers.len())
    }

    /// Record `journals` as learned by this operator (#246): from `init`'s
    /// reply or through `Inspect`, the only sources a call may name.
    fn note_learned(&self, journals: impl IntoIterator<Item = JournalIdentifier>) {
        self.learned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(journals);
    }

    /// The cell: the one this operator knows, or — while it knows none, or
    /// reached only some of its members — the one a majority of the
    /// founding members name now, learned through `Inspect` (§3.8), never
    /// from the harness.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn learn(&mut self, ctx: &SimContext) -> Option<Cell> {
        if let Some(cell) = &self.cell
            && cell.servers.len() == cell.members.len()
        {
            return Some(cell.clone());
        }
        // The cell a majority of the founding members serve: an address a
        // wiped member left may serve another cell now (#216).
        let Some((journals, _)) = bootstrap::majority_cell(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &self.machines[..self.layout.founders],
            self.patience,
        )
        .await
        else {
            return self.cell.clone();
        };
        let servers = bootstrap::discover(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &self.machines,
            self.patience,
        )
        .await;
        if servers.is_empty() {
            return self.cell.clone();
        }
        let client = self.connect(ctx, &servers);
        self.note_learned(
            [Some(journals.cell), journals.fleet, journals.election]
                .into_iter()
                .flatten(),
        );
        let Some(fleet) = journals.fleet else {
            assert_always!(
                false,
                "fleet: the cell init formed hosts the fleet tenant",
                { "cell" => journals.cell_id }
            );
            return self.cell.clone();
        };
        // An admitted machine serves no journal (#216): the first server
        // that serves the cell control journal names its members.
        let Some(members) = bootstrap::cell_members(&client, journals.cell).await else {
            return self.cell.clone();
        };
        assert_always!(
            crate::machine::formed_cell(ctx.state()) == Some(journals),
            "fleet: the control journals Inspect names are the ones init formed",
            { "cell" => journals.cell_id }
        );
        assert_reachable!("fleet: an operator learns the cell's control journals through Inspect");
        let servers: Vec<(u64, Address)> = servers
            .into_iter()
            .filter(|(id, _)| members.contains(id))
            .collect();
        if servers.is_empty() {
            return self.cell.clone();
        }
        if self
            .cell
            .as_ref()
            .map(|known| known.journals)
            .or(self.forgotten.take())
            .is_some_and(|known| known != journals)
        {
            // The init this client saw end was another cell's: a lost
            // cell's successor may still be initializing (#304's shape,
            // learned through Inspect instead of an unknown-journal
            // refusal; witness 9648373582943959266 of #529 (sets oracles
            // after PR #515), recipe 188857:3339894226954213537).
            self.initialized = false;
            self.forget_lost_universe();
        }
        self.cell = Some(Cell {
            journals,
            fleet,
            members,
            client: self.connect(ctx, &servers),
            servers,
        });
        self.cell.clone()
    }

    /// The cell this operator knew is lost and another formed at its
    /// founders' addresses: a new universe. A tenant operation it stopped
    /// in, and the identifiers it created, named the lost one's fleet
    /// directory; resumed here they would register an identifier a lost
    /// journal holds, which the audit keys journals by.
    fn forget_lost_universe(&mut self) {
        if matches!(
            self.pending,
            Some(super::Pending::Create(..) | super::Pending::Remove(_))
        ) {
            self.pending = None;
            self.killed = false;
        }
        self.ever_created.clear();
    }

    /// The cell an `init` came to (#246).
    fn adopt(&mut self, ctx: &SimContext, initialized: &Initialized) {
        let journals = initialized.journals;
        self.note_learned(
            [Some(journals.cell), journals.fleet, journals.election]
                .into_iter()
                .flatten(),
        );
        // A wipe after the reply may have lost the plan it names, and the
        // machines formed another since (#536 (cell init oracle reads a
        // stale board)): the reply was true when it came.
        assert_always!(
            crate::machine::formed_cell(ctx.state()) == Some(journals)
                || crate::machine::plan_lost(ctx.state(), journals.cell_id),
            "init: the cell init reports is the one the machines formed",
            { "cell" => journals.cell_id }
        );
        let Some(fleet) = journals.fleet else {
            assert_always!(
                false,
                "fleet: the cell init formed hosts the fleet tenant",
                { "cell" => journals.cell_id }
            );
            return;
        };
        if self
            .cell
            .as_ref()
            .map(|known| known.journals)
            .or(self.forgotten.take())
            .is_some_and(|known| known != journals)
        {
            self.forget_lost_universe();
        }
        self.cell = Some(Cell {
            journals,
            fleet,
            members: initialized.members.clone(),
            client: self.connect(ctx, &initialized.servers),
            servers: initialized.servers.clone(),
        });
    }

    /// `init` whole (#246, #277), as `parosctl init` runs it: `cell init`
    /// over the founding members the layout draws, sent to the first one
    /// still idle — from a drawn founder on its own BUGGIFY location, so two
    /// operators drive two decrees at once; with a second `cell init` sent
    /// to another founder alongside it on another; or, on a third, sent to
    /// a machine outside the founders, which must refuse it. Whether it
    /// ended: a misdirected `cell init` ends nothing, so the `init` an
    /// operator stopped in stays pending (witness 8900646803663139915: a
    /// resumption drawn misdirected dropped an `init` interrupted at the
    /// fleet half, and the cell stayed `REGISTERING`).
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn initialize(&mut self, ctx: &SimContext, draw: u64) -> bool {
        let founders = self.layout.founders;
        let mut members: Vec<Address> = self.machines[..founders].to_vec();
        crate::machine::note_init_sent(ctx.state());
        let outside = founders..self.machines.len();
        if !outside.is_empty() && buggify_with_prob!(0.05) {
            assert_reachable!("init: an operator sends init to a machine outside the seeds");
            let span = (outside.end - outside.start) as u64;
            let target =
                self.machines[outside.start + usize::try_from(draw % span).unwrap_or(0)].clone();
            self.misdirected(ctx, target, &members).await;
            return false;
        }
        if founders > 1 && buggify_with_prob!(0.25) {
            assert_reachable!("init: an operator starts cell init at another founder");
            members.rotate_left(1 + usize::try_from(draw % (founders as u64 - 1)).unwrap_or(0));
        }
        if crate::frontend::machines_pinned(ctx.state()) && buggify_with_prob!(0.1) {
            self.init_with_tenant_token(&members[0], &members).await;
        }
        // The listed machines past the founders, which `init` admits: the
        // founders are `2F+1` with `F = founders / 2`, so `init` forms the
        // cell on them alone (#523 (redundancy naming)).
        let failures_per_partition = u32::try_from(founders / 2).unwrap_or(u32::MAX);
        let listed: Vec<Address> = members
            .iter()
            .chain(&self.machines[founders..self.layout.listed])
            .cloned()
            .collect();
        if listed.len() > founders {
            assert_reachable!("init: an operator lists more machines than found the cell");
        }
        let wiped = crate::machine::other_cells_wiped(ctx.state());
        let connector = self.connector.clone();
        let token =
            crate::frontend::operator_token(ctx.state(), connector.providers().time().now());
        let observer: Arc<dyn paros::client::CallObserver> = Arc::new(Announce::every(ctx));
        let whole = paros::client::initialize::initialize(
            connector.providers(),
            connector.rpc(),
            connector.names(),
            &listed,
            |servers| {
                connector
                    .client(servers)
                    .with_observer(observer.clone())
                    .rotating_over(servers.len())
            },
            InitParams {
                patience: self.patience,
                fleet_id: draw | 1,
                leader_seed: self.leader_seeds.next(),
                names: self.init_names.clone(),
                token: token.clone(),
                root_key: Some(crate::frontend::signing_entry(ctx.state())),
                failures_per_partition,
                idle: false,
            },
        );
        let run = if founders > 1 && buggify_with_prob!(0.25) {
            // A second `cell init` at once, at another founder: the two
            // decrees converge on one cell (#277).
            assert_reachable!("init: a second cell init runs at another founder");
            let other =
                members[1 + usize::try_from(draw % (founders as u64 - 1)).unwrap_or(0)].clone();
            let second = bootstrap::cell_init(
                connector.providers(),
                connector.rpc(),
                connector.names(),
                &other,
                &members,
                (&token, false),
                self.patience,
            );
            // Judged as it resolves, not once the whole `init` ends: wipes in
            // between change the board it is judged against (#536 (cell init
            // oracle reads a stale board), witness
            // `replay-recipe 18203814145639737644 16953:385774492864553900` on
            // 616e83f).
            let second = async {
                let outcome = second.await;
                judge_second(ctx, &other, &members, &outcome, wiped);
            };
            let (run, ()) = futures::future::join(whole, second).await;
            run
        } else {
            whole.await
        };
        self.judge(ctx, run, founders, wiped)
    }

    /// On a pinned seed (#245 (Biscuit tokens)), a `cell init` with a
    /// tenant's token: a pinned machine refuses it, and nothing forms.
    async fn init_with_tenant_token(&self, target: &Address, members: &[Address]) {
        let connector = &self.connector;
        let now = connector.providers().time().now();
        let token = crate::frontend::token(
            &self.state,
            "acme",
            "",
            crate::frontend::Presented::Own,
            now,
        );
        let outcome = bootstrap::cell_init(
            connector.providers(),
            connector.rpc(),
            connector.names(),
            target,
            members,
            (&token, false),
            self.patience.min(Duration::from_secs(5)),
        )
        .await;
        assert_always!(
            !matches!(outcome, InitOutcome::Formed(_)),
            "init: a pinned machine never forms a cell for a tenant token"
        );
        if matches!(&outcome, InitOutcome::Refused(label) if label == "forbidden") {
            assert_reachable!("init: a pinned machine refuses cell init for a tenant token");
        }
    }

    /// What a whole `init` came to, judged; whether it ended.
    fn judge(&mut self, ctx: &SimContext, run: InitRun, founders: usize, wiped: u64) -> bool {
        match run {
            InitRun::Initialized(initialized) | InitRun::AlreadyInitialized(initialized) => {
                initialized.steps.iter().copied().for_each(reach);
                // Admissions of the listed machines past the founders
                // follow the fleet steps.
                let finished = initialized.steps.contains(&Stage::CellReady);
                assert_always!(
                    initialized.members.len() == founders,
                    "init: the cell forms on the founding members alone",
                    { "founders" => founders, "members" => initialized.members.len() }
                );
                if !initialized.admitted.is_empty() {
                    assert_reachable!("init: init admits the listed machines past the founders");
                }
                if finished {
                    assert_sometimes!(
                        initialized.steps.first() != Some(&Stage::FormFleet),
                        "fleet: an init resumes from where the journals stand"
                    );
                }
                assert_sometimes!(finished, "fleet: an init registers the cell READY");
                if initialized.steps.is_empty() {
                    assert_reachable!("init: a re-run finds nothing left to do");
                } else {
                    assert_reachable!("init: a run initializes the fleet");
                }
                self.adopt(ctx, &initialized);
                self.initialized = true;
                true
            }
            InitRun::Refused(InitRefusal::Formation(label)) if label == "storage" => {
                // The receiver's write failed under it: nothing was decided
                // that a re-run does not find in its ask.
                assert_reachable!("init: a seed's failed write refuses init, and it is run again");
                false
            }
            InitRun::Refused(InitRefusal::Formation(label)) if label == "cell_lost" => {
                // A majority of the plan's founding members were wiped after
                // a vote named them (#246): no plan can be chosen over the
                // listed addresses, and a re-run changes nothing.
                assert_always!(
                    crate::machine::cell_lost(ctx.state()),
                    "init: init is refused as cell_lost only once a majority of founders was wiped",
                    { "founders" => founders }
                );
                assert_reachable!("init: a cell that lost a majority of its founders refuses init");
                true
            }
            InitRun::Refused(InitRefusal::Formation(label)) if label == "other_cell_init" => {
                // A founder's address now belongs to another cell an
                // operator founded (#216), and no vote of this run's plan
                // survives: the founders can no longer be one cell.
                assert_always!(
                    crate::machine::founder_in_other_cell(ctx.state(), wiped),
                    "init: init is refused as other_cell_init only once another cell holds a founder",
                    { "founders" => founders }
                );
                assert_reachable!("init: a founder's address in another cell refuses init");
                true
            }
            InitRun::Refused(InitRefusal::Fleet(FleetRefusal::OtherCell { cell_id, .. })) => {
                // A listed machine past the founders serves another cell:
                // one cell per run, unless a wiped one-founder cell formed
                // again over it (#246), as for `cell add-machine`.
                assert_always!(
                    crate::machine::founder_wiped(ctx.state()),
                    "init: a listed machine of another cell exists only after a wipe",
                    { "cell" => cell_id }
                );
                true
            }
            InitRun::Refused(InitRefusal::Fleet(FleetRefusal::Retired { node })) => {
                // A listed machine past the founders that an operator
                // removed (#359): `init` registered it, and a drain removed
                // it before the admission landed, so its id is a tombstone.
                // A retired id never comes back; the operator's re-run lists
                // a machine it removed (witness: the recipe
                // 16995329085795718639 14013:17259961563028630674 on
                // aff9a7e).
                assert_always!(
                    crate::machine::remove_asked(ctx.state(), node.0),
                    "init: a listed machine is refused as retired only once an operator removed it",
                    { "node" => node.0 }
                );
                assert_reachable!("init: a removed listed machine refuses init");
                true
            }
            InitRun::Refused(refusal) => {
                assert_always!(
                    false,
                    "init: a run on the seeds is refused only for a failed write",
                    { "refusal" => format!("{refusal:?}") }
                );
                true
            }
            InitRun::Unreachable(_) | InitRun::Interrupted(_) => {
                assert_reachable!("init: a run that decided nothing in time is run again");
                false
            }
        }
    }

    /// A `cell init` sent to a machine outside the founders: refused as not a
    /// member, or as `cell_exists` once `cell add-machine` admitted it (#216),
    /// and nothing forms.
    async fn misdirected(&self, ctx: &SimContext, target: Address, members: &[Address]) {
        let outcome = bootstrap::cell_init(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &target,
            members,
            (
                &crate::frontend::operator_token(
                    ctx.state(),
                    self.connector.providers().time().now(),
                ),
                false,
            ),
            self.patience,
        )
        .await;
        match outcome {
            InitOutcome::Refused(label) if label == "cell_exists" => {
                assert_always!(
                    crate::machine::is_admitted(ctx.state(), &target)
                        || crate::machine::was_wiped(ctx.state(), &target),
                    "init: a machine outside the seeds refuses init as not a seed",
                    { "refusal" => label.as_str() }
                );
            }
            InitOutcome::Refused(label) => {
                assert_always!(
                    label == "not_a_member",
                    "init: a machine outside the seeds refuses init as not a seed",
                    { "refusal" => label.as_str() }
                );
                assert_reachable!("init: a machine outside the seeds refuses init");
            }
            InitOutcome::Formed(plan) => {
                assert_always!(
                    false,
                    "init: a machine outside the seeds never initializes the cell",
                    { "cell" => plan.cell_id }
                );
            }
            InitOutcome::NotWaiting | InitOutcome::Malformed | InitOutcome::Unreachable => {}
        }
    }
}

/// The second `cell init` of a run that sent two at once: it forms the
/// one cell the machines formed, or decides nothing — never another. Sent
/// to a machine `cell add-machine` admitted at `other` (#423), it is refused
/// as `cell_exists`: that machine is in the cell already.
///
/// `cell_exists` comes from any listed member a cell admitted, not only from
/// `other`: the decree's fan-out refuses on the first member that answers
/// `in_cell` (`paros::machine::cell_init`), such as a wiped founder's
/// replacement the cell admitted back.
fn judge_second(
    ctx: &SimContext,
    other: &Address,
    members: &[Address],
    second: &InitOutcome,
    wiped: u64,
) {
    match second {
        InitOutcome::Formed(plan) => {
            assert_always!(
                crate::machine::formed_cell(ctx.state()) == Some(plan.control_journals())
                    || crate::machine::plan_lost(ctx.state(), plan.cell_id),
                "init: two concurrent cell inits converge on one cell",
                { "cell" => plan.cell_id }
            );
            assert_reachable!("init: a second concurrent cell init finishes the one cell");
        }
        InitOutcome::Refused(label) => {
            assert_always!(
                label == "storage"
                    || (label == "cell_lost" && crate::machine::cell_lost(ctx.state()))
                    || (label == "other_cell_init"
                        && crate::machine::founder_in_other_cell(ctx.state(), wiped))
                    || (label == "cell_exists"
                        && std::iter::once(other)
                            .chain(members)
                            .any(|member| crate::machine::is_admitted(ctx.state(), member))),
                "init: a second concurrent cell init is refused only for a failed write",
                { "refusal" => label.as_str() }
            );
        }
        InitOutcome::NotWaiting | InitOutcome::Malformed | InitOutcome::Unreachable => {}
    }
}

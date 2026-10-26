//! `TAG` (#575 (key:value tags)): an operator sets the user tags of a
//! journal, a tenant, a cell or a machine, whole, through the library's
//! calls `parosctl` prints: a `SetTags` request to the tenant coordinator
//! (`paros::client::journals`), `FleetSession::set_tags` in the universe
//! directory, `CellSession::set_machine_tags` in the cell control journal.
//! Journal and tenant creates draw their birth tags here too.
//!
//! The run's tag board records every tag set any client asked for, before
//! it sends the request. The oracles: what a fold or a view shows as an
//! entity's user tags is a set some client asked for it, or none; no user
//! tag holds a reserved key; a tenant scope's view of a machine shows only
//! its zone and rack; a retried request acts once. On an untagged seed
//! (`crate::shape::untagged`) no tag is drawn and `TAG` does nothing, and
//! every other oracle of the run still holds: no code reads a user tag to
//! decide.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::StateHandle;
use moonpool_sim::{
    RandomProvider, SimContext, assert_always, assert_reachable, assert_sometimes,
    buggify_with_prob,
};
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::fleet::{Step, TagTarget, read_directory};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::core::NodeId;
use paros::core::TenantId;
use paros::model::fleet::{TenantKind, TenantState};
use paros::model::registry::NodeStanding;
use paros::model::tags::{MAX_TAGS, Tag, Tags, UserTags};
use paros::wire::common as wire;
use paros::wire::view as view_wire;

use super::FleetOps;

/// The keys and values a draw picks from: few, so two requests race for
/// one target often.
const KEYS: [&str; 3] = ["team", "env", "tier"];
const VALUES: [&str; 3] = ["a", "b", "c"];

/// What a tag request names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::chain_workload) enum Target {
    /// A tenant's journal, by name: a name created again is one target.
    Journal(TenantId, Vec<u8>),
    Tenant(TenantId),
    Cell(u64),
    Machine(u64),
}

/// Every tag set a client asked for, per target.
#[derive(Default)]
struct Board {
    asked: BTreeMap<Target, BTreeSet<Vec<(String, String)>>>,
}

fn board(state: &StateHandle) -> std::sync::Arc<std::sync::Mutex<Board>> {
    crate::state::published(state, "paros-sim/tags", Board::default)
}

fn pairs(tags: &Tags) -> Vec<(String, String)> {
    tags.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Record that a client asks `target` to carry `tags`: before the request
/// is sent, so a reader never sees a set the board lacks.
pub(in crate::chain_workload) fn note(state: &StateHandle, target: Target, tags: &UserTags) {
    board(state)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .asked
        .entry(target)
        .or_default()
        .insert(pairs(tags.tags()));
}

/// The user tags a create or a `TAG` request carries: none on an untagged
/// seed; else zero to three from the alphabet, or on its own location the
/// most a request may carry.
pub(in crate::chain_workload) fn drawn(state: &StateHandle) -> UserTags {
    if crate::shape::untagged(state) {
        return UserTags::none();
    }
    let tags: Vec<Tag> = if buggify_with_prob!(0.05) {
        assert_reachable!("tags: a request carries the most user tags");
        (0..MAX_TAGS)
            .filter_map(|i| Tag::new(format!("k{i}"), "v").ok())
            .collect()
    } else {
        let count = moonpool_sim::sim_random_range(0_usize..4);
        let mut held = BTreeMap::new();
        for _ in 0..count {
            let key = KEYS[moonpool_sim::sim_random_range(0..KEYS.len())];
            let value = VALUES[moonpool_sim::sim_random_range(0..VALUES.len())];
            held.insert(key, value);
        }
        held.into_iter()
            .filter_map(|(k, v)| Tag::new(k, v).ok())
            .collect()
    };
    Tags::of(tags)
        .and_then(UserTags::new)
        .unwrap_or_else(|_| UserTags::none())
}

/// Judge the user tags `held` a fold or a view shows for `target`.
pub(in crate::chain_workload) fn judge(state: &StateHandle, target: &Target, held: &Tags) {
    let held = pairs(held);
    assert_always!(
        held.iter()
            .all(|(k, _)| !paros::model::tags::is_reserved(k)),
        "tags: a user tag never holds a reserved key"
    );
    assert_always!(
        held.len() <= MAX_TAGS,
        "tags: an entity carries at most the most user tags",
        { "held" => held.len() }
    );
    let known = board(state)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .asked
        .get(target)
        .is_some_and(|asked| asked.contains(&held));
    assert_always!(
        held.is_empty() || known,
        "tags: an entity's user tags are a set some client asked for",
        { "held" => held.len() }
    );
}

/// The user tags of a view's tag list (its reserved ones dropped).
fn user_of(tags: &[wire::Tag]) -> Option<Tags> {
    let user: Vec<wire::Tag> = tags
        .iter()
        .filter(|t| !paros::model::tags::is_reserved(&t.key))
        .cloned()
        .collect();
    Tags::from_wire(user).ok()
}

/// Judge every tag a view answer shows (#575): the user tags as a fold's,
/// the reserved ones known keys only, and under a tenant scope a machine's
/// zone and rack alone.
pub(in crate::chain_workload) fn judge_view(
    state: &StateHandle,
    reply: &view_wire::ViewReply,
    admin: bool,
) {
    use paros::model::tags::reserved;
    let known = [
        reserved::KIND,
        reserved::ROLE,
        reserved::MOVABLE,
        reserved::CLASS,
        reserved::ZONE,
        reserved::RACK,
        reserved::REGION,
        reserved::REGIONS,
        reserved::WITNESS,
    ];
    let all = reply
        .machines
        .iter()
        .flat_map(|m| &m.tags)
        .chain(reply.tenants.iter().flat_map(|t| &t.tags))
        .chain(
            reply
                .tenants
                .iter()
                .flat_map(|t| &t.journals)
                .flat_map(|j| &j.tags),
        )
        .chain(reply.cells.iter().flat_map(|c| &c.tags));
    for tag in all {
        assert_always!(
            !paros::model::tags::is_reserved(&tag.key) || known.contains(&tag.key.as_str()),
            "tags: a view shows only known reserved tags"
        );
    }
    for m in &reply.machines {
        if admin {
            assert_always!(
                paros::model::tags::value_of(&m.tags, reserved::CLASS).is_some(),
                "tags: an admin view shows every machine's class",
                { "node" => m.node_id }
            );
            if let Some(user) = user_of(&m.tags) {
                judge(state, &Target::Machine(m.node_id), &user);
            }
        } else {
            assert_always!(
                m.tags
                    .iter()
                    .all(|t| t.key == reserved::ZONE || t.key == reserved::RACK),
                "tags: a tenant scope sees a machine's zone and rack only",
                { "node" => m.node_id }
            );
        }
    }
    for t in &reply.tenants {
        if t.kind == "users"
            && let Some(user) = user_of(&t.tags)
        {
            judge(state, &Target::Tenant(TenantId(t.tenant)), &user);
        }
        for j in &t.journals {
            if !j.name.is_empty()
                && let Some(user) = user_of(&j.tags)
            {
                judge(
                    state,
                    &Target::Journal(TenantId(t.tenant), j.name.clone()),
                    &user,
                );
            }
        }
    }
    for c in &reply.cells {
        if let Some(user) = user_of(&c.tags) {
            judge(state, &Target::Cell(c.cell_id), &user);
        }
    }
}

impl FleetOps {
    /// `TAG`: set the user tags of a journal, a tenant, a cell or a
    /// machine, drawn from `draw`.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn tag(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        if crate::shape::untagged(&self.state) {
            assert_reachable!("tags: an untagged run sets no tag");
            return;
        }
        match draw % 4 {
            0 => self.tag_journal(ctx, draw >> 2).await,
            1 => self.tag_directory(ctx, policy, draw >> 2, false).await,
            2 => self.tag_directory(ctx, policy, draw >> 2, true).await,
            _ => self.tag_machine(ctx, policy, draw >> 2).await,
        }
    }

    /// Set a live journal's tags, or on a coin a name no live journal
    /// holds, which must be refused `unknown_journal`.
    async fn tag_journal(&mut self, ctx: &SimContext, draw: u64) {
        let Some((cell, tenant, control, _)) = self.journal_tenant(ctx, draw).await else {
            return;
        };
        let Some(election) = cell.journals.election else {
            return;
        };
        let Some(fold) = journals::list(&cell.client, tenant, control.journal).await else {
            return;
        };
        let live: Vec<Vec<u8>> = fold.live().map(|(_, j)| j.name.clone()).collect();
        let name = if live.is_empty() || buggify_with_prob!(0.1) {
            b"nobody".to_vec()
        } else {
            live[usize::try_from(draw % live.len() as u64).unwrap_or(0)].clone()
        };
        let tags = drawn(&self.state);
        let target = Target::Journal(tenant, name.clone());
        note(&self.state, target.clone(), &tags);
        let request = JournalRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            op: JournalOp::SetTags {
                name: name.clone(),
                tags: tags.clone(),
            },
        };
        let answer = self.ask_coordinator(&cell, election, &request).await;
        match &answer {
            JournalAnswer::Tagged { id } => {
                assert_reachable!("tags: a journal's tags are set");
                if let Some(fold) = journals::list(&cell.client, tenant, control.journal).await
                    && let Some(journal) = fold.journals().find(|(j, _)| j == id).map(|(_, j)| j)
                {
                    judge(&self.state, &target, journal.tags.tags());
                    assert_sometimes!(
                        journal.tags == tags,
                        "tags: a read-back shows the tags a client set"
                    );
                }
            }
            JournalAnswer::UnknownJournal => {
                assert_reachable!("tags: a tag request for no live journal is refused");
            }
            _ => {}
        }
        if !answer.is_retryable() && buggify_with_prob!(0.2) {
            let again = self.ask_coordinator(&cell, election, &request).await;
            if !again.is_retryable() && again != JournalAnswer::UnknownTenant {
                assert_always!(
                    again == answer,
                    "tags: a retried tag request reads back its first answer",
                    { "first" => answer.as_str(), "again" => again.as_str() }
                );
                assert_reachable!("tags: a retried tag request acts once");
            }
        }
    }

    /// Set a `READY` users tenant's tags, or with `cell` the cell's, in the
    /// universe directory.
    async fn tag_directory(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
        cell_target: bool,
    ) {
        let Some(cell) = self.learn(ctx).await else {
            return;
        };
        let first = cell.first(draw);
        let Ok(directory) = read_directory(&cell.client, first, cell.fleet).await else {
            return;
        };
        let (name, target) = if cell_target {
            let Some((id, _)) = directory.cells().next() else {
                return;
            };
            (directory.cell_name(id).to_vec(), Target::Cell(id))
        } else {
            let ready: Vec<(TenantId, Vec<u8>)> = directory
                .tenants()
                .filter(|(_, t)| t.state == TenantState::Ready && t.kind == TenantKind::Users)
                .map(|(id, t)| (id, t.name.clone()))
                .collect();
            if ready.is_empty() {
                return;
            }
            let (id, name) = ready[usize::try_from(draw % ready.len() as u64).unwrap_or(0)].clone();
            (name, Target::Tenant(id))
        };
        if name.is_empty() {
            return;
        }
        let Some(mut session) = self.session(&cell, cell.journals, policy) else {
            return;
        };
        let tags = drawn(&self.state);
        note(&self.state, target.clone(), &tags);
        let request = ctx.random().random_range(1..u64::MAX);
        let on = if cell_target {
            TagTarget::Cell(&name)
        } else {
            TagTarget::Tenant(&name)
        };
        let client = cell.client.clone();
        let run = session
            .set_tags(&client, first, on, request, &tags, self.patience)
            .await;
        run.steps.iter().copied().for_each(super::reach);
        if !matches!(run.outcome, Step::Done { .. }) {
            return;
        }
        if cell_target {
            assert_reachable!("tags: a cell's tags are set");
        } else {
            assert_reachable!("tags: a tenant's tags are set");
        }
        if let Ok(directory) = read_directory(&client, first, cell.fleet).await {
            let held = match &target {
                Target::Cell(id) => directory.cell(*id).map(|c| c.tags.clone()),
                Target::Tenant(id) => directory.tenant(*id).map(|t| t.tags.clone()),
                _ => None,
            };
            if let Some(held) = held {
                judge(&self.state, &target, held.tags());
            }
        }
        if buggify_with_prob!(0.2) {
            // A retry of a decided request acts once: the directory holds
            // the request applied, so no step writes.
            let again = session
                .set_tags(&client, first, on, request, &tags, self.patience)
                .await;
            if matches!(again.outcome, Step::Done { .. }) {
                assert_always!(
                    again.steps.is_empty(),
                    "tags: a retried tag request writes nothing",
                    { "steps" => again.steps.len() }
                );
                assert_reachable!("tags: a retried tag request acts once");
            }
        }
    }

    /// Set a registered machine's tags in the cell control journal.
    async fn tag_machine(&mut self, ctx: &SimContext, policy: CheckpointPolicy, draw: u64) {
        let Some(cell) = self.learn(ctx).await else {
            return;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, paros::model::address::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            self.leader_seeds.next(),
            policy,
        );
        let first = cell.first(draw);
        if session.open(&cell.client, first).await.is_err() {
            return;
        }
        let registered: Vec<NodeId> = session
            .registry()
            .nodes()
            .filter(|(_, n)| n.standing != NodeStanding::Retired)
            .map(|(id, _)| id)
            .collect();
        if registered.is_empty() {
            return;
        }
        let node = registered[usize::try_from(draw % registered.len() as u64).unwrap_or(0)];
        let tags = drawn(&self.state);
        let target = Target::Machine(node.0);
        note(&self.state, target.clone(), &tags);
        let request = ctx.random().random_range(1..u64::MAX);
        if let Ok(true) = session
            .set_machine_tags(&cell.client, first, (node, request), &tags)
            .await
        {
            assert_reachable!("tags: a machine's tags are set");
            if let Some(held) = session.registry().get(node) {
                judge(&self.state, &target, held.tags.tags());
            }
        }
    }
}

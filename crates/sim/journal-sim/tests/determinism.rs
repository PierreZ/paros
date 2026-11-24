//! Each walk is deterministic: one seed run twice gives the same log. A
//! model that reads a clock, a `HashMap` order or a generator of its own
//! fails here (moonpool-walk's `check_determinism`).

use journal_sim::{client, controller, election, fold, heal, host, register, roles, whole};
use moonpool_walk::{Model, Walk};

/// The seeds that each walk runs twice.
const SEEDS: [u64; 2] = [1, 2];

/// Each seed passes, and runs twice to the same log: two runs that fail
/// the same way are equal too, so the seed must pass first.
fn deterministic<M: Model>(name: &str, walk: &Walk<M>) {
    for seed in SEEDS {
        if let Err(failure) = walk.replay(seed) {
            panic!("the {name} walk fails at seed {seed}: {failure}");
        }
        walk.check_determinism(seed).unwrap_or_else(|why| {
            panic!("the {name} walk is not deterministic at seed {seed}: {why}")
        });
    }
}

#[test]
fn the_fold_walk_is_deterministic() {
    for shape in [fold::Shape::TRACED, fold::Shape::WIDE] {
        let walk = Walk::<fold::FoldModel>::new()
            .seeds(1)
            .chaos_steps(120)
            .model(move |cx| fold::FoldModel::build(shape, fold::FoldMutant::None, cx));
        deterministic("fold", &walk);
    }
}

#[test]
fn the_register_walk_is_deterministic() {
    for shape in [register::Shape::TRACED, register::Shape::WIDE] {
        deterministic(
            "register",
            &register::walk(shape, register::RegisterMutant::None, 1, 300),
        );
    }
}

#[test]
fn the_roles_walk_is_deterministic() {
    for shape in [roles::Shape::NARROW, roles::Shape::WIDE] {
        deterministic(
            "roles",
            &roles::walk(shape, roles::RolesMutant::None, 1, 600),
        );
    }
}

#[test]
fn the_driver_walk_is_deterministic() {
    deterministic("driver", &host::walk(host::HostMutant::None, 1, 600));
}

#[test]
fn the_client_walk_is_deterministic() {
    deterministic("client", &client::walk(client::ClientMutant::None, 1));
}

#[test]
fn the_election_walk_is_deterministic() {
    deterministic(
        "election",
        &election::walk(election::ElectionMutant::None, 1),
    );
}

#[test]
fn the_controller_walk_is_deterministic() {
    for shape in [controller::Shape::TRACED, controller::Shape::WIDE] {
        deterministic(
            "controller",
            &controller::walk(shape, controller::ControllerMutant::None, 1, 300),
        );
    }
}

#[test]
fn the_heal_walk_is_deterministic() {
    deterministic("heal", &heal::walk(heal::HealMutant::None, 1, 300));
}

#[test]
fn the_whole_walk_is_deterministic() {
    deterministic("whole", &whole::walk(whole::WholeMutant::None, 1, 1500));
}

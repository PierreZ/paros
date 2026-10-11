use super::*;

fn config(members: &[u64], quorum: QuorumSystem) -> AcceptorConfig {
    AcceptorConfig::new(members.iter().map(|id| NodeId(*id)).collect(), quorum)
}

fn standing(gone: &[u64], leaving: &[u64]) -> impl Fn(NodeId) -> Member {
    let (gone, leaving) = (gone.to_vec(), leaving.to_vec());
    move |id: NodeId| {
        if gone.contains(&id.0) {
            Member::Gone
        } else if leaving.contains(&id.0) {
            Member::Leaving
        } else {
            Member::Serving
        }
    }
}

#[test]
fn the_desired_shape_with_every_member_serving_is_healthy() {
    let placed = config(&[1, 2, 3], QuorumSystem::Majority);
    let word = judge(&placed, 3, QuorumSystem::Majority, standing(&[], &[]));
    assert_eq!(word, Word::Healthy);
}

#[test]
fn a_member_down_with_both_quorums_left_is_degraded() {
    let placed = config(&[1, 2, 3], QuorumSystem::Majority);
    assert_eq!(
        judge(&placed, 3, QuorumSystem::Majority, standing(&[2], &[])),
        Word::Degraded
    );
    // A draining member still votes, but the shape must move off it.
    assert_eq!(
        judge(&placed, 3, QuorumSystem::Majority, standing(&[], &[3])),
        Word::Degraded
    );
    // Another shape than the desired one.
    assert_eq!(
        judge(&placed, 5, QuorumSystem::Majority, standing(&[], &[])),
        Word::Degraded
    );
}

#[test]
fn no_live_quorum_of_a_phase_is_unavailable() {
    let placed = config(&[1, 2, 3], QuorumSystem::Majority);
    assert_eq!(
        judge(&placed, 3, QuorumSystem::Majority, standing(&[1, 2], &[])),
        Word::Unavailable
    );
    // A grid: a full row is a Phase-1 quorum and a full column a Phase-2
    // one. Losing a whole column leaves no Phase-2 quorum.
    let grid = QuorumSystem::Grid { rows: 2, cols: 2 };
    let placed = config(&[1, 2, 3, 4], grid);
    assert_eq!(judge(&placed, 4, grid, standing(&[1], &[])), Word::Degraded);
    assert_eq!(
        judge(&placed, 4, grid, standing(&[1, 3], &[])),
        Word::Unavailable
    );
}

#[test]
fn a_single_journal_compares_shapes_only_until_zones() {
    // #215 (zones) makes a `single` journal Degraded; until then its shape
    // alone decides.
    let placed = config(&[7], QuorumSystem::Majority);
    assert_eq!(
        judge(&placed, 1, QuorumSystem::Majority, standing(&[], &[])),
        Word::Healthy
    );
    assert_eq!(
        judge(&placed, 1, QuorumSystem::Majority, standing(&[7], &[])),
        Word::Unavailable
    );
}

#[test]
fn hysteresis_shows_a_new_word_only_once_it_held() {
    let second = Duration::from_secs(1);
    let mut status = Hysteresis::new(2 * second);
    let key = (1, 2);
    // The first word is shown at once.
    assert_eq!(
        status.show(key, Word::Healthy, Duration::ZERO),
        Word::Healthy
    );
    // A new word is held back until it held for the window.
    assert_eq!(status.show(key, Word::Degraded, second), Word::Healthy);
    assert_eq!(status.show(key, Word::Degraded, 2 * second), Word::Healthy);
    assert_eq!(status.show(key, Word::Degraded, 3 * second), Word::Degraded);
    // A flap back and forth inside the window never changes the word.
    assert_eq!(status.show(key, Word::Healthy, 4 * second), Word::Degraded);
    assert_eq!(status.show(key, Word::Degraded, 5 * second), Word::Degraded);
    assert_eq!(status.show(key, Word::Healthy, 6 * second), Word::Degraded);
    assert_eq!(
        status.show(key, Word::Unavailable, 7 * second),
        Word::Degraded
    );
    // A forgotten journal starts again.
    status.retain(&BTreeSet::new());
    assert_eq!(
        status.show(key, Word::Unavailable, 8 * second),
        Word::Unavailable
    );
}

#[test]
fn a_zero_hold_shows_every_word() {
    let mut status = Hysteresis::new(Duration::ZERO);
    assert_eq!(
        status.show((1, 1), Word::Healthy, Duration::ZERO),
        Word::Healthy
    );
    assert_eq!(
        status.show((1, 1), Word::Degraded, Duration::ZERO),
        Word::Degraded
    );
}

#[test]
fn a_word_label_parses_back() {
    for word in [Word::Healthy, Word::Degraded, Word::Unavailable] {
        assert_eq!(Word::parse(word.as_str()), Some(word));
    }
    assert_eq!(Word::parse("fine"), None);
}

#[test]
fn a_machine_stands_by_its_view() {
    let machine = |up: bool, standing: &str| wire::MachineView {
        up,
        standing: standing.to_string(),
        ..wire::MachineView::default()
    };
    assert_eq!(member(None), Member::Gone);
    assert_eq!(member(Some(&machine(false, "registered"))), Member::Gone);
    assert_eq!(member(Some(&machine(true, "retired"))), Member::Gone);
    assert_eq!(member(Some(&machine(true, "draining"))), Member::Leaving);
    assert_eq!(member(Some(&machine(true, "founding"))), Member::Serving);
}

use super::*;

#[test]
fn a_fresh_role_gives_ids_from_zero() {
    let mut flight = Flight::after(None);
    assert_eq!(flight.last(), None);
    assert_eq!(flight.batch(), PersistId(0));
    assert_eq!(flight.last(), Some(PersistId(0)));
    assert_eq!(flight.batch(), PersistId(1));
    assert_eq!(flight.last(), Some(PersistId(1)));
}

#[test]
fn a_restarted_role_gives_ids_above_its_disk() {
    // A reply with no write waits on the durable batch, and a new batch is
    // above it: an id given again would let a send wait on a batch of the
    // earlier life.
    let mut flight = Flight::after(Some(PersistId(5)));
    assert_eq!(flight.last(), Some(PersistId(5)));
    assert_eq!(flight.batch(), PersistId(6));
    assert_eq!(flight.last(), Some(PersistId(6)));
}

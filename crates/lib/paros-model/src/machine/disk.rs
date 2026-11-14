//! **A machine's disk as placement counts it** (#985 (placement counts store
//! disk), `docs/architecture/03-2-machines.md` §3.2, decided on 2026-11-11).
//!
//! Every store paros opens (a journal's acceptor store, a replica's store, a
//! matchmaker set's store) allocates its first segment whole when it is
//! created: 64 MiB in the default geometry of #600 (drop the small store
//! layout), before it holds one record. A machine reports two facts with its
//! class and locality: the bytes of its disk that paros may fill, and what
//! one store allocates. The registry counts one store per booking that opens
//! a store ([`crate::registry::Booking::opens_store`]), and refuses a
//! booking that the disk cannot hold.
//!
//! The machine computes `store_bytes` from its own store configuration
//! (`JournalStoreConfig::store_bytes` in `paros-storage`), never from a
//! second constant here: this layer sits below the stores and knows no
//! geometry.

use paros_wire::machine as wire;

/// What a machine reports of its disk (#985 (placement counts store disk)).
///
/// `total_bytes == 0` or `store_bytes == 0` is a disk the machine did not
/// report (a registration older than #985, or a machine that could not
/// read its disk): it holds any number of stores, as before.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Disk {
    /// The bytes of the disk that paros may fill: `PAROS_DISK_BYTES`, else
    /// the size of the file system under the data directory.
    pub total_bytes: u64,
    /// The bytes one new store allocates before it holds a record.
    pub store_bytes: u64,
}

impl Disk {
    /// A disk the machine did not report: no limit.
    pub const UNKNOWN: Disk = Disk {
        total_bytes: 0,
        store_bytes: 0,
    };

    /// A disk of `total_bytes` whose stores each allocate `store_bytes`.
    #[must_use]
    pub fn new(total_bytes: u64, store_bytes: u64) -> Self {
        Self {
            total_bytes,
            store_bytes,
        }
    }

    /// Whether the machine reported both facts: only then does the
    /// registry count stores on it.
    #[must_use]
    pub fn is_known(&self) -> bool {
        self.total_bytes > 0 && self.store_bytes > 0
    }

    /// How many stores the disk holds in all, or `None` when it is not
    /// known (no limit).
    #[must_use]
    pub fn stores(&self) -> Option<u64> {
        self.is_known().then(|| self.total_bytes / self.store_bytes)
    }

    /// How many more stores the disk holds once `open` stores are booked
    /// on it, or `None` when it is not known (no limit). A disk that holds
    /// fewer stores than are booked (a machine restarted with a smaller
    /// disk) holds `0` more; the stores already booked stay.
    #[must_use]
    pub fn room(&self, open: u64) -> Option<u64> {
        self.stores().map(|stores| stores.saturating_sub(open))
    }

    /// The disk an `IdentifyAck` reports.
    #[must_use]
    pub fn of_ack(ack: &wire::IdentifyAck) -> Self {
        Self::new(ack.disk_bytes, ack.store_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn an_unreported_disk_holds_any_number_of_stores() {
        assert_eq!(Disk::UNKNOWN.stores(), None);
        assert_eq!(Disk::UNKNOWN.room(u64::MAX), None);
        assert_eq!(Disk::new(0, 64 * MIB).room(3), None);
        assert_eq!(Disk::new(100 * MIB, 0).room(3), None);
        assert!(!Disk::default().is_known());
    }

    #[test]
    fn a_disk_holds_its_size_over_the_store_cost_rounded_down() {
        let disk = Disk::new(100 * 1024 * MIB, 64 * MIB);
        assert!(disk.is_known());
        assert_eq!(disk.stores(), Some(1600));
        assert_eq!(Disk::new(64 * MIB - 1, 64 * MIB).stores(), Some(0));
        assert_eq!(Disk::new(64 * MIB, 64 * MIB).stores(), Some(1));
        assert_eq!(Disk::new(3 * 64 * MIB - 1, 64 * MIB).stores(), Some(2));
    }

    #[test]
    fn room_is_what_is_left_and_never_below_zero() {
        let disk = Disk::new(4 * 64 * MIB, 64 * MIB);
        assert_eq!(disk.room(0), Some(4));
        assert_eq!(disk.room(3), Some(1));
        assert_eq!(disk.room(4), Some(0));
        assert_eq!(disk.room(9), Some(0));
    }

    #[test]
    fn an_ack_reports_its_disk() {
        let ack = wire::IdentifyAck {
            disk_bytes: 7,
            store_bytes: 3,
            ..wire::IdentifyAck::default()
        };
        assert_eq!(Disk::of_ack(&ack), Disk::new(7, 3));
    }
}

//! The **register port**: the journal's compare-and-set register (§J3.4),
//! as the driver reaches it.
//!
//! A role asks a change as an effect; the driver runs it here and gives the
//! role the value of a done write, or the refusal. A role acts only on a
//! value that a done write gave (§J3.4, "act on a write seen done"), so
//! [`read`](Register::read) gives only such a value.

use paros_consensus::journal::JournalId;
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};

/// Why a change gave no done value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeError {
    /// The register refused the change: its guard failed, so the write did
    /// not take place.
    Refused(ChangeRefusal),
    /// The register holds no journal of this id: no write took place.
    Unknown,
    /// No answer came (a lost request or a lost reply): the write may or
    /// may not have taken place, and a minority may hold it. The caller
    /// asks the same change again. The register takes a change that its
    /// value already holds "ok" again and writes the value again (§J3.4, "a
    /// change asked again"), so a second ask that finds the first write
    /// makes it done; a refusal says that the value holds another change.
    NoAnswer,
}

/// The registers of the journals that a process serves.
///
/// In one process the register is a value in memory
/// ([`crate::mem::MemRegister`]). In a deployment it runs on the journal's
/// matchmakers (`paros_journal::register::RegisterCall`), and this trait is
/// its client. **Open** (#1330 (paros-journal-moonpool)): the register on
/// the matchmakers as roles of the [`Host`](crate::Host).
pub trait Register {
    /// Run `change` on the register of `journal`: the value of the done
    /// write, or the refusal.
    ///
    /// # Errors
    ///
    /// The register refused the change (its guard failed), it holds no
    /// journal of this id, or no answer came ([`ChangeError`]).
    fn change(&mut self, journal: JournalId, change: &Change)
    -> Result<RegisterValue, ChangeError>;

    /// The value of the last done write on the register of `journal`, if
    /// the register exists.
    fn read(&mut self, journal: JournalId) -> Option<RegisterValue>;
}

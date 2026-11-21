//! The **register port**: the journal's compare-and-set register (§J3.4),
//! as the driver reaches it.
//!
//! A role asks a change as an effect; the driver runs it here and gives the
//! role the value of a done write, or the refusal. A role acts only on a
//! value that a done write gave (§J3.4, "act on a write seen done"), so
//! [`read`](Register::read) gives only such a value.

use paros_consensus::journal::JournalId;
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};

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
    /// The register refused the change (its guard failed).
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeRefusal>;

    /// The value of the last done write on the register of `journal`, if
    /// the register exists.
    fn read(&mut self, journal: JournalId) -> Option<RegisterValue>;
}

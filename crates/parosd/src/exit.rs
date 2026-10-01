//! What a role's exit tells the supervisor: one exit code per
//! [`RunError`] variant, so a process manager restarts what a restart
//! fixes and stops on what it does not.
//!
//! The codes are the BSD `sysexits.h` ones, which systemd and most
//! supervisors already know:
//!
//! | Exit | Code | Meaning | Supervisor |
//! |---|---|---|---|
//! | clean shutdown (`SIGTERM`, `SIGINT`) | 0 | | done |
//! | [`RunError::Storage`] | 75 (`EX_TEMPFAIL`) | the fail-stop crash a storage fault decides: recovery is the next boot | restart |
//! | [`RunError::Refused`] | 78 (`EX_CONFIG`) | the boot claim and the store disagree (#147): an operator decides | do not restart |
//! | [`RunError::Infra`] | 70 (`EX_SOFTWARE`) | bind, listen, address: fatal | do not restart |
//! | [`RunError::SeamCrash`] | 70 | only a simulation's hooks inject one | — |
//! | an invalid command line or topology | 64 (`EX_USAGE`) | | do not restart |

use std::process::ExitCode;

use paros::RunError;

/// A clean shutdown.
pub const OK: u8 = 0;
/// The command line or the topology it names is invalid.
pub const USAGE: u8 = 64;
/// A fatal infrastructure failure.
pub const SOFTWARE: u8 = 70;
/// A storage fault: restart the process.
pub const TEMPFAIL: u8 = 75;
/// A boot refused on the operator's claim: an operator acts.
pub const CONFIG: u8 = 78;

/// The exit code of a role's run.
#[must_use]
pub fn code(result: &Result<(), RunError>) -> u8 {
    match result {
        Ok(()) => OK,
        Err(RunError::Storage(_)) => TEMPFAIL,
        Err(RunError::Refused(_)) => CONFIG,
        Err(RunError::Infra(_) | RunError::SeamCrash(_)) => SOFTWARE,
    }
}

/// Log a role's outcome and turn it into the process's exit.
#[must_use]
pub fn report(role: &str, result: &Result<(), RunError>) -> ExitCode {
    match result {
        Ok(()) => tracing::info!(role, "parosd_stopped"),
        Err(error) => tracing::error!(role, %error, code = code(result), "parosd_exited"),
    }
    ExitCode::from(code(result))
}

#[cfg(test)]
mod tests {
    use paros::{BootRefusal, Seam, StorageError, StorageRecord, WriteOutcome};

    use super::*;

    #[test]
    fn each_run_error_has_its_own_supervisor_answer() {
        assert_eq!(code(&Ok(())), OK);
        assert_eq!(
            code(&Err(RunError::Storage(StorageError::Io {
                record: StorageRecord::Batch,
                outcome: WriteOutcome::Lost,
            }))),
            TEMPFAIL
        );
        assert_eq!(code(&Err(RunError::Refused(BootRefusal::Amnesia))), CONFIG);
        assert_eq!(
            code(&Err(RunError::Infra(
                moonpool_core::SimulationError::InvalidState("bind".into())
            ))),
            SOFTWARE
        );
        assert_eq!(code(&Err(RunError::SeamCrash(Seam::BeforeSync))), SOFTWARE);
    }
}

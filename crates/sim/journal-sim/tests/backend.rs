//! The backends of the walks keep the contracts of the traits they fake:
//! a fake that breaks its contract tests nothing.

use journal_sim::backend::{LossyRegister, QuorumRegister};
use paros_journal::register::RegisterValue;
use paros_journal_io::contract;

/// The register on three matchmakers, with its members up and no fault
/// armed, keeps the register contract, at several draws of the members
/// that each operation reaches.
#[test]
fn the_quorum_register_keeps_the_register_contract() {
    for draw in 1..=16 {
        contract::register(|journal| {
            let register = QuorumRegister::new(|_| {}, || {}, draw);
            register.create(journal, RegisterValue::new());
            register
        });
    }
}

/// The lossy register with no fault armed keeps the register contract.
#[test]
fn the_lossy_register_keeps_the_register_contract() {
    contract::register(|journal| {
        let register = QuorumRegister::new(|_| {}, || {}, 7);
        register.create(journal, RegisterValue::new());
        LossyRegister::new(register, |_| {})
    });
}

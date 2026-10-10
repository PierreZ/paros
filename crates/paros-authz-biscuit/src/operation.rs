//! The operations a request names, their classes and their access.
//!
//! A derived block checks a class, never an operation, so a token stays
//! valid when an operation joins an existing class. The names are the
//! strings the verifier adds as facts; never rename one, since tokens in
//! the field name them.

/// What an operation does to its target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// It changes nothing.
    Read,
    /// It changes something.
    Write,
}

impl Access {
    /// The fact's value: `access("read")`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// A group of operations, the unit a policy grants and a block restricts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// The data plane of one tenant.
    Data,
    /// The journals of one tenant.
    Journal,
    /// The tenant's own view of its spread over its cell (#399).
    TenantView,
    /// Tenant creation, deletion and listing (admin only: tenant tokens
    /// are minted offline).
    TenantAdmin,
    /// The machines, cells and full detail (#399, admin only).
    CellView,
    /// Universe administration.
    UniverseAdmin,
}

impl Class {
    /// The fact's value: `op_class("data")`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Journal => "journal",
            Self::TenantView => "tenant-view",
            Self::TenantAdmin => "tenant-admin",
            Self::CellView => "cell-view",
            Self::UniverseAdmin => "universe-admin",
        }
    }
}

macro_rules! operations {
    ($($(#[$doc:meta])* $variant:ident = $name:literal, $class:ident, $access:ident;)*) => {
        /// One call a token may be checked for.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Operation {
            $($(#[$doc])* $variant,)*
        }

        impl Operation {
            /// Every operation, in declaration order.
            pub const ALL: &[Operation] = &[$(Operation::$variant,)*];

            /// The fact's value: `operation("journal.write")`.
            #[must_use]
            pub fn name(self) -> &'static str {
                match self { $(Self::$variant => $name,)* }
            }

            /// The class a block or a policy names.
            #[must_use]
            pub fn class(self) -> Class {
                match self { $(Self::$variant => Class::$class,)* }
            }

            /// Whether the operation changes something.
            #[must_use]
            pub fn access(self) -> Access {
                match self { $(Self::$variant => Access::$access,)* }
            }
        }
    };
}

operations! {
    /// Resolve a tenant name to its cell.
    TenantResolve = "tenant.resolve", Data, Read;
    /// Read records.
    JournalRead = "journal.read", Data, Read;
    /// Follow a journal.
    JournalTail = "journal.tail", Data, Read;
    /// Write records.
    JournalWrite = "journal.write", Data, Write;
    /// Truncate a journal.
    JournalTruncate = "journal.truncate", Data, Write;
    /// Change a journal's writer.
    JournalSetLeader = "journal.set-leader", Data, Write;
    /// Create a journal.
    JournalCreate = "journal.create", Journal, Write;
    /// Delete a journal.
    JournalDelete = "journal.delete", Journal, Write;
    /// List a tenant's journals.
    JournalList = "journal.list", Journal, Read;
    /// Show one journal.
    JournalShow = "journal.show", Journal, Read;
    /// Show a tenant's spread over its cell.
    TenantShow = "tenant.show", TenantView, Read;
    /// Show who holds the roles of a tenant.
    RolesTenant = "roles.tenant", TenantView, Read;
    /// Create a tenant.
    TenantCreate = "tenant.create", TenantAdmin, Write;
    /// Delete a tenant.
    TenantDelete = "tenant.delete", TenantAdmin, Write;
    /// List the tenants.
    TenantList = "tenant.list", TenantAdmin, Read;
    /// The full detail of a view: asked before a view's own operation.
    ViewDetail = "view.detail", CellView, Read;
    /// List a cell's machines.
    MachineList = "machine.list", CellView, Read;
    /// Show one machine.
    MachineShow = "machine.show", CellView, Read;
    /// List the cells.
    CellList = "cell.list", CellView, Read;
    /// Show one cell.
    CellShow = "cell.show", CellView, Read;
    /// Show who holds the roles of a cell.
    RolesCell = "roles.cell", CellView, Read;
    /// Show who holds the roles on a machine.
    RolesMachine = "roles.machine", CellView, Read;
    /// Each server's view of a node or a journal.
    Inspect = "inspect", CellView, Read;
    /// Form the universe.
    Init = "init", UniverseAdmin, Write;
    /// Form a cell.
    CellForm = "cell.form", UniverseAdmin, Write;
    /// Admit a cell.
    CellAdmit = "cell.admit", UniverseAdmin, Write;
    /// Admit a machine into a cell.
    MachineAdmit = "machine.admit", UniverseAdmin, Write;
    /// Drain a machine.
    MachineDrain = "machine.drain", UniverseAdmin, Write;
    /// Retire a machine.
    MachineRetire = "machine.retire", UniverseAdmin, Write;
    /// Add a root public key to the universe entry.
    UniverseKeyAdd = "universe.key.add", UniverseAdmin, Write;
    /// Remove a root public key from the universe entry.
    UniverseKeyRemove = "universe.key.remove", UniverseAdmin, Write;
    /// Change a journal's acceptor set.
    Reconfigure = "reconfigure", UniverseAdmin, Write;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_names_are_unique() {
        let mut names: Vec<_> = Operation::ALL.iter().map(|op| op.name()).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count);
    }
}

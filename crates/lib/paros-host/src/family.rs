//! The **family** of a host: the types that every role of one host shares,
//! and the few facts about them that the host and the driver need.
//!
//! A host serves many roles of many kinds in one process (§J4.2, rule 3).
//! The roles of one host share one address type, one message type, one
//! disk format and one kind of batch id, so the driver can route, hold and
//! commit them without knowing what they are. A family is a type with no
//! value (a marker), as the toy family of the tests is.

use core::fmt::Debug;

/// The types that every role of one host shares.
///
/// The host never reads inside them: an address is opaque, a message is
/// routed only by [`route`](Family::route), an op is stored as it is, and a
/// batch id is only compared.
pub trait Family: Copy + Debug + Eq {
    /// The address of one role in the process: an opaque actor address. A
    /// journal role is one kind of address; a coordinator or a frontend is
    /// another.
    type Address: Copy + Ord + Debug;
    /// A message between two roles, as the transport carries it.
    type Envelope: Clone + Debug + Eq;
    /// The id of one persist batch of one role (rule 1). The ids of a role
    /// grow, across a restart too.
    type Batch: Copy + Ord + Debug;
    /// One op of a persist batch, in the format of the family.
    type Op: Clone + Debug + Eq;
    /// The records of one role, as a scan of the disk gives them.
    type Records: Clone + Debug + Eq;
    /// What placement asks the process to serve (the `Assignments` port,
    /// rule 5).
    type Assignment: Clone + Debug + Eq;
    /// An input of one role that runs: a message, or an input from outside
    /// the host that names the role.
    type Input: Clone + Debug + Eq;
    /// An effect of a role that leaves the host through the driver's
    /// [`Ports`](crate::Ports): a reply to a client, a call to a register, a
    /// notice.
    type Output: Clone + Debug + Eq;
    /// Why a role stopped.
    type Stop: Clone + Debug + Eq;

    /// The role that `assignment` names.
    fn address(assignment: &Self::Assignment) -> Self::Address;

    /// The role that receives `envelope`, and the input it steps on.
    fn route(envelope: Self::Envelope) -> (Self::Address, Self::Input);

    /// The highest batch that `records` hold as durable.
    fn durable(records: &Self::Records) -> Option<Self::Batch>;

    /// `records` with `ops` written on top, in order, as the disk holds them
    /// once the ops commit. A role that starts again before the driver's
    /// commit reads its own last writes this way.
    #[must_use]
    fn overlay(records: Self::Records, ops: &[Self::Op]) -> Self::Records;

    /// Whether the driver holds `output` until the batch it names is
    /// durable (`true`), or drops it when the batch is not durable yet
    /// (`false`: the role sends it again later, as a periodic report).
    fn holds(output: &Self::Output) -> bool {
        let _ = output;
        true
    }

    /// Whether a role that stopped for `why` keeps its batches in the
    /// driver's group, so they still reach the disk: a role that ends by its
    /// own step. Any other stop takes them out.
    fn keeps_batches(why: &Self::Stop) -> bool {
        let _ = why;
        false
    }

    /// The stop of a role that placement unassigned.
    fn unassigned() -> Self::Stop;

    /// The stop of a role whose disk could not be read
    /// ([`StorageError::Full`](crate::StorageError::Full) on its scan): it
    /// may start again.
    fn unreadable() -> Self::Stop;

    /// The stop of a role whose disk lost a record
    /// ([`StorageError::Lost`](crate::StorageError::Lost)): it stops for
    /// good. A scan of it reports the loss each time, so it never starts
    /// again from what the loss left.
    fn lost() -> Self::Stop;

    /// The stop of a role whose batch was in a storage commit that wrote
    /// nothing ([`StorageError::Full`](crate::StorageError::Full), rule 4):
    /// it may start again from a scan.
    fn failed() -> Self::Stop;
}

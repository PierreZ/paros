//! The acceptor's side of a scrub (#362 (background scrubbing),
//! `docs/architecture/05-failure-model-and-zones.md` §5): one page of a journal's durable records,
//! read back from the device and checked by the node that holds them.
//!
//! The tenant's elected scrubber (`paros_machine::scrub`) asks each
//! acceptor for the same range, never a quorum read: a quorum read returns
//! one good copy and hides the rot on the other replicas. The node checks
//! every record's checksum locally ([`LogStorage::scrub`]) and answers each
//! record's ballot and value fingerprint, so the values never cross the
//! network. A record that fails its check goes to the node's protocol as
//! CTRL's faulty ([`paros_core::ColocatedNode::repair_record`]): the node
//! still holds the vote it cast (its store loaded and checked it at boot, or
//! recorded it in this incarnation), so the acceptor writes it again from its
//! own copy and the loop's settle tail makes it durable before the answer
//! leaves. A node that rebooted onto the damage reports the slot faulty at
//! boot instead, and CTRL repairs it from its peers.

use paros_core::{ColocatedNode, Slot};

use paros_storage::{LogStorage, ScrubbedSlot};
pub use paros_tunables::scrub::{SCRUB_PAGE, SCRUB_PAGE_BYTES};
use paros_wire::{ScrubReply, ScrubRequest, ballot_to_proto, internal};

/// What one page did on this node: the answer, and whether the core holds
/// repairs the settle tail must make durable first.
pub(crate) struct Served {
    pub(crate) reply: ScrubReply,
    pub(crate) repairs: usize,
}

/// Read and check one page of `storage` for `req`, and hand each damaged
/// record to `node`'s protocol. A failed device read answers `unavailable`
/// and changes nothing: a scrub never quarantines a journal.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0, from = req.from_slot))]
pub(crate) async fn serve<S: LogStorage>(
    node: &mut ColocatedNode,
    storage: &mut S,
    req: &ScrubRequest,
) -> Served {
    let limit = usize::try_from(req.limit)
        .unwrap_or(SCRUB_PAGE)
        .clamp(1, SCRUB_PAGE);
    let max_bytes = req.max_bytes.clamp(1, SCRUB_PAGE_BYTES);
    let page = match storage.scrub(Slot(req.from_slot), limit, max_bytes).await {
        Ok(page) => page,
        Err(error) => {
            tracing::warn!(node = node.config().id.0, %error, "scrub_read_failed");
            return Served {
                reply: refused("unavailable"),
                repairs: 0,
            };
        }
    };
    assert!(
        page.slots.len() <= limit,
        "a scrub page stays inside its limit"
    );
    let mut repairs = 0;
    let slots = page
        .slots
        .iter()
        .map(|(slot, scrubbed)| match *scrubbed {
            ScrubbedSlot::Intact { ballot, digest } => internal::ScrubbedSlot {
                slot: slot.0,
                ballot: Some(ballot_to_proto(ballot)),
                digest,
                damaged: false,
                repaired: false,
            },
            ScrubbedSlot::Damaged { ballot } => {
                let repaired = node.repair_record(*slot).is_some();
                repairs += usize::from(repaired);
                tracing::warn!(
                    node = node.config().id.0,
                    slot = slot.0,
                    repaired,
                    "scrub_found_damage"
                );
                internal::ScrubbedSlot {
                    slot: slot.0,
                    ballot: Some(ballot_to_proto(ballot)),
                    digest: 0,
                    damaged: true,
                    repaired,
                }
            }
        })
        .collect();
    let first_unchosen = node
        .replica()
        .chosen_index()
        .map_or(0, |chosen| chosen.0 + 1);
    Served {
        reply: ScrubReply {
            refusal: String::new(),
            slots,
            next_slot: page.next.map(|next| next.0),
            bytes: page.bytes,
            first_unchosen,
            first_slot: node.acceptor().first_slot().0,
        },
        repairs,
    }
}

/// A refused page.
pub(crate) fn refused(refusal: &str) -> ScrubReply {
    ScrubReply {
        refusal: refusal.to_string(),
        ..ScrubReply::default()
    }
}

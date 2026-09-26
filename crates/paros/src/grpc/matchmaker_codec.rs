//! The matchmaker wire: every matchmaker request and reply to and from its
//! typed protobuf.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorConfig, Ballot, GcAck, GcRequest, MatchOutcome, MatchRefusal, MatchReply, MatchRequest,
    MatchmakerGeneration, MatchmakerId, MatchmakerPhase, MatchmakerSet, NodeId, PendingBootstrap,
    ReconfigureReply, ReconfigureRequest, Registration, RegistrationKind,
};

use super::codec::{
    ballot_from_proto, ballot_to_proto, config_from_proto, config_to_proto, unique_map,
};
use super::{
    WireGarbageCollect, WireGarbageCollectAck, WireMatchReply, WireMatchRequest,
    WireReconfigureReply, WireReconfigureRequest, common, matchmaker,
};

/// A configuration that must be present: the matchmaker contract carries no
/// optional configuration, so absence is an error rather than `None`. The
/// conversion itself is [`config_from_proto`], shared with the consensus wire.
fn acceptor_config_from_proto(
    config: Option<common::AcceptorConfig>,
) -> Result<AcceptorConfig, &'static str> {
    config_from_proto(config)?.ok_or("missing acceptor configuration")
}

fn mm_set_to_proto(set: &MatchmakerSet) -> matchmaker::MatchmakerSet {
    matchmaker::MatchmakerSet {
        generation: set.generation.0,
        members: set.members().iter().map(|m| m.0).collect(),
    }
}

/// A matchmaker set that is already present. Protobuf wraps every message
/// field in an `Option`, and a caller that has unwrapped it should not have to
/// wrap it again just to reach the conversion.
fn mm_set_value(set: matchmaker::MatchmakerSet) -> Result<MatchmakerSet, &'static str> {
    if set.members.is_empty() {
        return Err("empty matchmaker set");
    }
    Ok(MatchmakerSet::new(
        MatchmakerGeneration(set.generation),
        set.members.into_iter().map(MatchmakerId).collect(),
    ))
}

fn mm_set_from_proto(
    set: Option<matchmaker::MatchmakerSet>,
) -> Result<MatchmakerSet, &'static str> {
    mm_set_value(set.ok_or("missing matchmaker set")?)
}

fn registrations_to_proto(
    history: &BTreeMap<Ballot, Registration>,
) -> Vec<matchmaker::Registration> {
    history
        .iter()
        .map(|(ballot, registration)| matchmaker::Registration {
            ballot: Some(ballot_to_proto(*ballot)),
            config: Some(config_to_proto(&registration.config)),
            reconfiguration: registration.kind.is_reconfiguration(),
        })
        .collect()
}

fn registrations_from_proto(
    entries: Vec<matchmaker::Registration>,
) -> Result<BTreeMap<Ballot, Registration>, &'static str> {
    unique_map(
        entries.into_iter().map(|entry| {
            let ballot = ballot_from_proto(entry.ballot)?;
            // The wire keeps the flag a bool; the kind is the core's word for
            // it, mapped at the boundary.
            let registration = Registration {
                config: acceptor_config_from_proto(entry.config)?,
                kind: if entry.reconfiguration {
                    RegistrationKind::Reconfiguration
                } else {
                    RegistrationKind::Belief
                },
            };
            Ok((ballot, registration))
        }),
        "duplicate ballot in history",
    )
}

/// Encode a matchmaking request for the wire.
#[must_use]
pub(crate) fn wire_match_request(request: &MatchRequest) -> WireMatchRequest {
    WireMatchRequest {
        from: request.from.0,
        ballot: Some(ballot_to_proto(request.ballot)),
        config: Some(config_to_proto(&request.config)),
        reconfiguration: request.kind.is_reconfiguration(),
        generation: request.generation.0,
        from_ballot: request.from_ballot.map(ballot_to_proto),
    }
}

/// Validate and decode a matchmaking request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn match_request_from_wire(
    request: WireMatchRequest,
) -> Result<MatchRequest, &'static str> {
    let from = NodeId(request.from);
    let ballot = ballot_from_proto(request.ballot)?;
    let config = acceptor_config_from_proto(request.config)?;
    let generation = MatchmakerGeneration(request.generation);
    let base = if request.reconfiguration {
        MatchRequest::reconfigure(from, ballot, config, generation)
    } else {
        MatchRequest::new(from, ballot, config, generation)
    };
    Ok(match request.from_ballot {
        Some(cursor) => base.from_page(ballot_from_proto(Some(cursor))?),
        None => base,
    })
}

/// Encode a matchmaker's reply for the wire.
#[must_use]
pub(crate) fn wire_match_reply(reply: &MatchReply) -> WireMatchReply {
    let outcome = match &reply.outcome {
        MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        } => matchmaker::match_reply::Outcome::Registered(matchmaker::Registered {
            history: registrations_to_proto(history),
            gc_watermark: Some(ballot_to_proto(*gc_watermark)),
            effective: effective_to_proto(effective.as_ref()),
            from_ballot: Some(ballot_to_proto(*from_ballot)),
            next_from_ballot: next_from_ballot.map(ballot_to_proto),
        }),
        MatchOutcome::Refused(refusal) => {
            let reason = match refusal {
                MatchRefusal::Stale { highest } => {
                    matchmaker::refused::Reason::StaleHighest(ballot_to_proto(*highest))
                }
                MatchRefusal::BelowWatermark { watermark } => {
                    matchmaker::refused::Reason::BelowWatermark(ballot_to_proto(*watermark))
                }
                MatchRefusal::Stopped { successor } => {
                    matchmaker::refused::Reason::Stopped(matchmaker::RefusedStopped {
                        successor: successor.as_ref().map(mm_set_to_proto),
                    })
                }
                MatchRefusal::Generation { current } => {
                    matchmaker::refused::Reason::Generation(mm_set_to_proto(current))
                }
                MatchRefusal::Inactive => {
                    matchmaker::refused::Reason::Inactive(matchmaker::RefusedInactive {})
                }
            };
            matchmaker::match_reply::Outcome::Refused(matchmaker::Refused {
                reason: Some(reason),
            })
        }
    };
    WireMatchReply {
        matchmaker: reply.matchmaker.0,
        to: reply.to.0,
        ballot: Some(ballot_to_proto(reply.ballot)),
        outcome: Some(outcome),
        generation: reply.generation.0,
    }
}

/// Validate and decode a matchmaker's reply from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn match_reply_from_wire(reply: WireMatchReply) -> Result<MatchReply, &'static str> {
    let outcome = match reply.outcome.ok_or("missing match outcome")? {
        matchmaker::match_reply::Outcome::Registered(registered) => MatchOutcome::Registered {
            from_ballot: ballot_from_proto(registered.from_ballot)?,
            history: registrations_from_proto(registered.history)?,
            next_from_ballot: registered
                .next_from_ballot
                .map(|b| ballot_from_proto(Some(b)))
                .transpose()?,
            gc_watermark: ballot_from_proto(registered.gc_watermark)?,
            effective: effective_from_proto(registered.effective)?,
        },
        matchmaker::match_reply::Outcome::Refused(refused) => {
            MatchOutcome::Refused(match refused.reason.ok_or("missing refusal reason")? {
                matchmaker::refused::Reason::StaleHighest(highest) => MatchRefusal::Stale {
                    highest: ballot_from_proto(Some(highest))?,
                },
                matchmaker::refused::Reason::BelowWatermark(watermark) => {
                    MatchRefusal::BelowWatermark {
                        watermark: ballot_from_proto(Some(watermark))?,
                    }
                }
                matchmaker::refused::Reason::Stopped(stopped) => MatchRefusal::Stopped {
                    successor: stopped.successor.map(mm_set_value).transpose()?,
                },
                matchmaker::refused::Reason::Generation(current) => MatchRefusal::Generation {
                    current: mm_set_value(current)?,
                },
                matchmaker::refused::Reason::Inactive(_) => MatchRefusal::Inactive,
            })
        }
    };
    Ok(MatchReply {
        matchmaker: MatchmakerId(reply.matchmaker),
        to: NodeId(reply.to),
        ballot: ballot_from_proto(reply.ballot)?,
        generation: MatchmakerGeneration(reply.generation),
        outcome,
    })
}

/// Encode a garbage-collection request for the wire.
#[must_use]
pub(crate) fn wire_garbage_collect(request: &GcRequest) -> WireGarbageCollect {
    WireGarbageCollect {
        from: request.from.0,
        watermark: Some(ballot_to_proto(request.watermark)),
        generation: request.generation.0,
    }
}

/// Decode a garbage-collection request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn garbage_collect_from_wire(
    request: WireGarbageCollect,
) -> Result<GcRequest, &'static str> {
    Ok(GcRequest {
        from: NodeId(request.from),
        generation: MatchmakerGeneration(request.generation),
        watermark: ballot_from_proto(request.watermark)?,
    })
}

/// Encode a garbage-collection acknowledgement for the wire.
#[must_use]
pub(crate) fn wire_garbage_collect_ack(ack: &GcAck) -> WireGarbageCollectAck {
    WireGarbageCollectAck {
        matchmaker: ack.matchmaker.0,
        watermark: Some(ballot_to_proto(ack.watermark)),
        generation: ack.generation.0,
        applied: ack.applied,
    }
}

/// Decode a garbage-collection acknowledgement.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn garbage_collect_ack_from_wire(
    ack: WireGarbageCollectAck,
) -> Result<GcAck, &'static str> {
    Ok(GcAck {
        matchmaker: MatchmakerId(ack.matchmaker),
        generation: MatchmakerGeneration(ack.generation),
        applied: ack.applied,
        watermark: ballot_from_proto(ack.watermark)?,
    })
}

/// Encode the effective configuration scalar (absent when nothing was ever
/// registered as a reconfiguration).
fn effective_to_proto(
    effective: Option<&(Ballot, AcceptorConfig)>,
) -> Option<matchmaker::EffectiveConfiguration> {
    effective.map(|(ballot, config)| matchmaker::EffectiveConfiguration {
        ballot: Some(ballot_to_proto(*ballot)),
        config: Some(config_to_proto(config)),
    })
}

/// Decode the effective configuration scalar.
fn effective_from_proto(
    effective: Option<matchmaker::EffectiveConfiguration>,
) -> Result<Option<(Ballot, AcceptorConfig)>, &'static str> {
    effective
        .map(|e| {
            Ok((
                ballot_from_proto(e.ballot)?,
                acceptor_config_from_proto(e.config)?,
            ))
        })
        .transpose()
}

fn bootstrap_to_proto(bootstrap: &PendingBootstrap) -> matchmaker::Bootstrap {
    matchmaker::Bootstrap {
        set: Some(mm_set_to_proto(&bootstrap.set)),
        gc_watermark: Some(ballot_to_proto(bootstrap.gc_watermark)),
        history: registrations_to_proto(&bootstrap.history),
        effective: effective_to_proto(bootstrap.effective.as_ref()),
    }
}

fn bootstrap_from_proto(
    bootstrap: matchmaker::Bootstrap,
) -> Result<PendingBootstrap, &'static str> {
    Ok(PendingBootstrap {
        set: mm_set_from_proto(bootstrap.set)?,
        gc_watermark: ballot_from_proto(bootstrap.gc_watermark)?,
        history: registrations_from_proto(bootstrap.history)?,
        effective: effective_from_proto(bootstrap.effective)?,
    })
}

fn phase_to_proto(phase: MatchmakerPhase) -> i32 {
    match phase {
        MatchmakerPhase::Fresh => matchmaker::MatchmakerPhase::Fresh,
        MatchmakerPhase::Inactive => matchmaker::MatchmakerPhase::Inactive,
        MatchmakerPhase::Active => matchmaker::MatchmakerPhase::Active,
        MatchmakerPhase::Stopped => matchmaker::MatchmakerPhase::Stopped,
    }
    .into()
}

fn phase_from_proto(phase: i32) -> Result<MatchmakerPhase, &'static str> {
    match matchmaker::MatchmakerPhase::try_from(phase) {
        Ok(matchmaker::MatchmakerPhase::Fresh) => Ok(MatchmakerPhase::Fresh),
        Ok(matchmaker::MatchmakerPhase::Inactive) => Ok(MatchmakerPhase::Inactive),
        Ok(matchmaker::MatchmakerPhase::Active) => Ok(MatchmakerPhase::Active),
        Ok(matchmaker::MatchmakerPhase::Stopped) => Ok(MatchmakerPhase::Stopped),
        Err(_) => Err("unknown matchmaker phase"),
    }
}

/// Encode a reconfigurer's request for the wire.
#[must_use]
pub(crate) fn wire_reconfigure_request(request: &ReconfigureRequest) -> WireReconfigureRequest {
    use matchmaker::reconfigure_request::Kind;
    let kind = match request {
        ReconfigureRequest::Stop { generation, .. } => Kind::Stop(matchmaker::Stop {
            generation: generation.0,
        }),
        ReconfigureRequest::Bootstrap { bootstrap, .. } => {
            Kind::Bootstrap(bootstrap_to_proto(bootstrap))
        }
        ReconfigureRequest::DecreePrepare {
            generation, ballot, ..
        } => Kind::DecreePrepare(matchmaker::DecreePrepare {
            generation: generation.0,
            ballot: Some(ballot_to_proto(*ballot)),
        }),
        ReconfigureRequest::DecreeAccept {
            generation,
            ballot,
            members,
            ..
        } => Kind::DecreeAccept(matchmaker::DecreeAccept {
            generation: generation.0,
            ballot: Some(ballot_to_proto(*ballot)),
            members: members.iter().map(|m| m.0).collect(),
        }),
        ReconfigureRequest::Chosen {
            generation,
            successor,
            ..
        } => Kind::Chosen(matchmaker::Chosen {
            generation: generation.0,
            successor: Some(mm_set_to_proto(successor)),
        }),
    };
    WireReconfigureRequest {
        from: request.from().0,
        kind: Some(kind),
    }
}

/// Validate and decode a reconfigurer's request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn reconfigure_request_from_wire(
    request: WireReconfigureRequest,
) -> Result<ReconfigureRequest, &'static str> {
    use matchmaker::reconfigure_request::Kind;
    let from = NodeId(request.from);
    Ok(
        match request.kind.ok_or("missing reconfigure request kind")? {
            Kind::Stop(stop) => ReconfigureRequest::Stop {
                from,
                generation: MatchmakerGeneration(stop.generation),
            },
            Kind::Bootstrap(bootstrap) => ReconfigureRequest::Bootstrap {
                from,
                bootstrap: bootstrap_from_proto(bootstrap)?,
            },
            Kind::DecreePrepare(prepare) => ReconfigureRequest::DecreePrepare {
                from,
                generation: MatchmakerGeneration(prepare.generation),
                ballot: ballot_from_proto(prepare.ballot)?,
            },
            Kind::DecreeAccept(accept) => {
                if accept.members.is_empty() {
                    return Err("empty decree proposal");
                }
                ReconfigureRequest::DecreeAccept {
                    from,
                    generation: MatchmakerGeneration(accept.generation),
                    ballot: ballot_from_proto(accept.ballot)?,
                    members: accept.members.into_iter().map(MatchmakerId).collect(),
                }
            }
            Kind::Chosen(chosen) => ReconfigureRequest::Chosen {
                from,
                generation: MatchmakerGeneration(chosen.generation),
                successor: mm_set_from_proto(chosen.successor)?,
            },
        },
    )
}

/// Encode a matchmaker's reconfiguration reply for the wire.
#[must_use]
pub(crate) fn wire_reconfigure_reply(reply: &ReconfigureReply) -> WireReconfigureReply {
    use matchmaker::reconfigure_reply::Kind;
    let kind = match reply {
        ReconfigureReply::Stopped {
            generation,
            gc_watermark,
            history,
            effective,
            successor,
            decree_promised,
            ..
        } => Kind::Stopped(matchmaker::StopAck {
            generation: generation.0,
            gc_watermark: Some(ballot_to_proto(*gc_watermark)),
            history: registrations_to_proto(history),
            successor: successor.as_ref().map(mm_set_to_proto),
            decree_promised: Some(ballot_to_proto(*decree_promised)),
            effective: effective_to_proto(effective.as_ref()),
        }),
        ReconfigureReply::Bootstrapped { set, .. } => {
            Kind::Bootstrapped(matchmaker::BootstrapAck {
                set: Some(mm_set_to_proto(set)),
            })
        }
        ReconfigureReply::Promised {
            generation,
            ballot,
            vote,
            ..
        } => Kind::Promised(matchmaker::DecreePromise {
            generation: generation.0,
            ballot: Some(ballot_to_proto(*ballot)),
            vote: vote.as_ref().map(|(b, members)| matchmaker::DecreeVote {
                ballot: Some(ballot_to_proto(*b)),
                members: members.iter().map(|m| m.0).collect(),
            }),
        }),
        ReconfigureReply::Accepted {
            generation, ballot, ..
        } => Kind::Accepted(matchmaker::DecreeAccepted {
            generation: generation.0,
            ballot: Some(ballot_to_proto(*ballot)),
        }),
        ReconfigureReply::Nacked {
            generation,
            ballot,
            promised,
            ..
        } => Kind::Nacked(matchmaker::DecreeNack {
            generation: generation.0,
            ballot: Some(ballot_to_proto(*ballot)),
            promised: Some(ballot_to_proto(*promised)),
        }),
        ReconfigureReply::Learned {
            generation,
            activated,
            at,
            ..
        } => Kind::Learned(matchmaker::Learned {
            generation: generation.0,
            activated: *activated,
            at: at.0,
        }),
        ReconfigureReply::Refused {
            current,
            phase,
            successor,
            ..
        } => Kind::Refused(matchmaker::ReconfigureRefused {
            current: Some(mm_set_to_proto(current)),
            phase: phase_to_proto(*phase),
            successor: successor.as_ref().map(mm_set_to_proto),
        }),
    };
    WireReconfigureReply {
        matchmaker: reply.matchmaker().0,
        kind: Some(kind),
    }
}

/// Validate and decode a matchmaker's reconfiguration reply from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub(crate) fn reconfigure_reply_from_wire(
    reply: WireReconfigureReply,
) -> Result<ReconfigureReply, &'static str> {
    use matchmaker::reconfigure_reply::Kind;
    let matchmaker = MatchmakerId(reply.matchmaker);
    Ok(match reply.kind.ok_or("missing reconfigure reply kind")? {
        Kind::Stopped(ack) => ReconfigureReply::Stopped {
            matchmaker,
            generation: MatchmakerGeneration(ack.generation),
            gc_watermark: ballot_from_proto(ack.gc_watermark)?,
            history: registrations_from_proto(ack.history)?,
            effective: effective_from_proto(ack.effective)?,
            successor: ack.successor.map(mm_set_value).transpose()?,
            decree_promised: ballot_from_proto(ack.decree_promised)?,
        },
        Kind::Bootstrapped(ack) => ReconfigureReply::Bootstrapped {
            matchmaker,
            set: mm_set_from_proto(ack.set)?,
        },
        Kind::Promised(promise) => ReconfigureReply::Promised {
            matchmaker,
            generation: MatchmakerGeneration(promise.generation),
            ballot: ballot_from_proto(promise.ballot)?,
            vote: promise
                .vote
                .map(|vote| {
                    if vote.members.is_empty() {
                        return Err("empty decree vote");
                    }
                    Ok((
                        ballot_from_proto(vote.ballot)?,
                        vote.members.into_iter().map(MatchmakerId).collect(),
                    ))
                })
                .transpose()?,
        },
        Kind::Accepted(accepted) => ReconfigureReply::Accepted {
            matchmaker,
            generation: MatchmakerGeneration(accepted.generation),
            ballot: ballot_from_proto(accepted.ballot)?,
        },
        Kind::Nacked(nack) => ReconfigureReply::Nacked {
            matchmaker,
            generation: MatchmakerGeneration(nack.generation),
            ballot: ballot_from_proto(nack.ballot)?,
            promised: ballot_from_proto(nack.promised)?,
        },
        Kind::Learned(learned) => ReconfigureReply::Learned {
            matchmaker,
            generation: MatchmakerGeneration(learned.generation),
            activated: learned.activated,
            at: MatchmakerGeneration(learned.at),
        },
        Kind::Refused(refused) => ReconfigureReply::Refused {
            matchmaker,
            current: mm_set_from_proto(refused.current)?,
            phase: phase_from_proto(refused.phase)?,
            successor: refused.successor.map(mm_set_value).transpose()?,
        },
    })
}

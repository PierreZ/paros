--------------------------------- MODULE MC ---------------------------------
EXTENDS QuorumEverywhere, TLC

\* Both configurations tile their members (`QuorumSystem::admits`).
ASSUME GridAdmits(OldSeq, Cols, Acks) /\ GridAdmits(NewSeq, Cols, Acks)
=============================================================================

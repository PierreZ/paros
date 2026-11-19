--------------------------------- MODULE Grid ---------------------------------
(***************************************************************************)
(* The quorums of `QuorumSystem::Grid { rows, cols, acks }` in            *)
(* `paros-consensus` (`membership/quorum_system.rs`), as sets. Other       *)
(* models take their quorums from here, so each model and the code agree   *)
(* on one layout (#1102 (rework 1.13)).                                    *)
(*                                                                         *)
(* A configuration is a tuple `M` of its members in sorted order. The      *)
(* layout is row-major: member `M[i]` sits at row `(i - 1) \div cols` and  *)
(* column `(i - 1) % cols`, so the layout is a function of the membership. *)
(* A change of members can move a member that stays to another column.     *)
(*                                                                         *)
(*   Phase 2: `acks` acceptors of one column (`is_phase2_quorum`).         *)
(*   Phase 1: `rows - acks + 1` acceptors of every column                  *)
(*            (`phase1_per_column`, `is_phase1_quorum`).                   *)
(*   A read:  the reader asks one row when `acks = rows` (`row_of`), and   *)
(*            the whole membership else. It completes on a Phase-1 quorum  *)
(*            of the answers (`QuorumRead`, `has_phase1_quorum_in`).        *)
(*                                                                         *)
(* The full grid of Compartmentalized Paxos is `acks = rows`. The          *)
(* partitions of #532 (acceptor partitions) are `rows = 2F + 1` and        *)
(* `acks = F + 1`.                                                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

GridMembers(M) == {M[i] : i \in DOMAIN M}

GridRows(M, cols) == Len(M) \div cols

\* The members of column j, 0 <= j < cols (`grid_column`).
GridColumn(M, cols, j) == {M[i] : i \in {k \in DOMAIN M : (k - 1) % cols = j}}

\* The members of row r, 0 <= r < rows (`grid_row`).
GridRow(M, cols, r) == {M[i] : i \in {k \in DOMAIN M : (k - 1) \div cols = r}}

\* What `QuorumSystem::admits` asks: the layout tiles the membership.
GridAdmits(M, cols, acks) ==
  /\ cols >= 1
  /\ Len(M) >= 1
  /\ Len(M) % cols = 0
  /\ acks \in 1..GridRows(M, cols)

\* The acceptors of each column that a Phase-1 quorum holds. `slack` is 0 in
\* every model; a control sets it to 1 to take one acceptor less.
GridPhase1Count(M, cols, acks, slack) == GridRows(M, cols) - acks + 1 - slack

GridQ1(M, cols, acks, slack) ==
  {Q \in SUBSET GridMembers(M) :
     \A j \in 0..(cols - 1) :
       Cardinality(Q \cap GridColumn(M, cols, j)) >= GridPhase1Count(M, cols, acks, slack)}

\* A Phase-2 quorum of column j: a slot is addressed to one column
\* (`column_of`, `slot % cols`).
GridQ2At(M, cols, acks, j) ==
  {Q \in SUBSET GridColumn(M, cols, j) : Cardinality(Q) >= acks}

\* A Phase-2 quorum of any column. A model that takes this set allows every
\* choice of column, the choice of `column_of` among them.
GridQ2(M, cols, acks) == UNION {GridQ2At(M, cols, acks, j) : j \in 0..(cols - 1)}

\* The sets of acceptors that a reader asks (`row_of`): each row on the full
\* grid, the whole membership else.
GridReadTargets(M, cols, acks) ==
  IF acks = GridRows(M, cols)
  THEN {GridRow(M, cols, r) : r \in 0..(GridRows(M, cols) - 1)}
  ELSE {GridMembers(M)}

\* The majority, for the models that compare it with a grid.
MajorityQuorums(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
=============================================================================

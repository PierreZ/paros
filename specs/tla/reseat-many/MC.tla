------------------------------- MODULE MC -----------------------------------
(* The proposed sets of the checks of `ReseatMany.tla`.                    *)
EXTENDS ReseatMany, TLC

CONSTANTS a, b, c, d

\* Grow: slot 1 replaces a by c, slot 2 replaces b by d.
PropGrow == [s \in {1, 2} |-> IF s = 1 THEN {b, c} ELSE {c, d}]

\* Back: slot 1 replaces a by c, slot 2 names a again.
PropBack == [s \in {1, 2} |-> IF s = 1 THEN {b, c} ELSE {a, b, c}]
=============================================================================

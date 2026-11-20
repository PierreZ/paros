--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS SegmentRecords, TLC

\* The set of generation 0 and its successor: X goes, W comes.
Old == {"X", "Y", "Z"}
New == {"Y", "Z", "W"}

\* The record of an open segment, of a retired one, and of a sealed one.
Open == <<0, 0, 0, 0>>
Retired == <<2, 1, 0, 0>>
Sealed == <<1, 1, 0, 0>>
\* The seal: two first leaders race, then the retire.
Seal == <<"seal1", "seal2", "retire">>
\* The offload: the offloader marks the record, and a replica that drops its
\* copy writes the mark again.
Offload == <<"offload", "drop">>
\* The controller: two elections, then the controller of term 1, deposed by
\* the second one, calls a reshape.
Control == <<"elect", "elect", "reshape1">>
\* Both at once: a retire, an election and a reshape of term 1, from a
\* sealed record.
Mixed == <<"retire", "elect", "reshape1">>
=============================================================================

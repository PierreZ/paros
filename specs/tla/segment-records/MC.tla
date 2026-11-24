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
\* A seal asked again: the first leader's seal, a race with a second leader,
\* and the first leader's seal again after its answer was lost.
SealAgain == <<"seal1", "seal2", "seal1">>
\* A seal asked again after a retire carried it: the first seal is taken by
\* a minority, the retire reads it and makes it done, then the first leader
\* asks its seal again.
RetireAgain == <<"seal1", "retire", "seal1">>
\* A retire asked again: the first retire is taken by a minority, or done
\* with its answer lost, and the leader asks it again.
RetireTwice == <<"retire", "retire">>
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

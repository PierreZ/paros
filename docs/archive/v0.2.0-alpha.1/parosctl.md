# parosctl

The operator and user CLI: one command per noun over `paros::client`, plus offline tools for keys, tokens, TLS and stores. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/bin/parosctl/` (13,397 lines of Rust).

## What it did

The top of the stack (`publish = false`, #220 (parosctl)). It depended on `paros` (with `tls`), `paros-authz-biscuit`, `moonpool-core` (compio providers) and `moonpool-rpc` (the client-only runtime). It was a library too (`src/lib.rs` exports `cmd` and `support`) so `paros-bench` could share the support code. Nothing else depended on it.

The rule (AGENTS.md "Local rules"): `parosctl` holds no client policy. Redirects, retries, claims, ambiguity, reader resume and the patience of `init` live in `paros::client`. A command parses, calls the library, prints, and maps an `Ending` to an exit code.

Command families:

- **Bootstrap and topology.** `init` (cell formation plus universe, or `--idle` for a cell alone; `paros::client::initialize`), `universe key list|add|remove`, `universe add-cell|remove-cell|status|buckets` (#232 (multiple cells), #713 (bucket placement)), `cell add-machine|drain-machine|remove-machine|maintenance|tag|tag-machine|configure|balance` (#216, #474, #359, #685, #687).
- **Tenants and journals.** `tenant create|delete|tag|list|show` (`paros::client::fleet`), `journal create|delete|tag|list` and `configure` (`paros::client::journals`, one request id per run, re-sent by the library until decided).
- **Data plane.** `write`, `read`, `tail`, `truncate`, `set-leader`, `inspect`, `reconfigure`, `retire` (`src/cmd/data.rs`), directly or through frontends with a token (`--frontends`, `PAROS_TOKEN`, #192 (the frontend)).
- **Views.** `status` (#191 (status)), `machine list|show`, `cell list|show`, `tenant list|show`, `roles` (#399 (names in views)), `resolve` (#216). Text tables or `--json`.
- **Offline tools** (no server, no runtime). `key generate|show`, `token mint|derive|inspect` (#400 (keygen and offline tokens)), `tls init|ca|issue|show|verify` (PR #509 (mutual TLS)), `store inspect <data-dir>` (#365 (offline store inspection)).

Global options: `--servers` (`PAROSCTL_SERVERS`), `--json`, `--timeout-ms`, `--leader` (`PAROSCTL_LEADER`; drawn at random when absent, never zero, #241), `--frontends`, `--token`/`--token-file`, `--as-tenant`, `--tls-*` and `--tls-*-pem`.

Exit codes (`Ending` in `src/support/connect.rs` and `src/main.rs`): `0` success, `3` refused or not served, `4` ambiguous, `5` no server answered, `2` bad arguments, `1` no servers configured. `store inspect` exits `3` when a part does not read.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/bin/parosctl/src/main.rs` | The clap command tree, global options, dispatch, exit codes. |
| `crates/bin/parosctl/src/support/connect.rs` | `Global`, `Runtime` (compio providers, `RpcHandle`, `Names`), `runtime`, `Ending` and its `ExitCode` mapping. |
| `crates/bin/parosctl/src/cmd/data.rs` | One fn per data command (`write`, `read`, `tail`, `truncate`, ...); `writes_again`, `wrote`, `appended`, `truncated`, `parse_uuid`, `parse_quorum`. |
| `crates/bin/parosctl/src/cmd/init.rs` | `init` over `paros::client::initialize`; members default to the servers. |
| `crates/bin/parosctl/src/cmd/tenant.rs` | Tenant commands, `FleetRefusal` texts, `Interrupted` endings shared with `init`. |
| `crates/bin/parosctl/src/cmd/journal.rs` | Journal commands and `configure` (layouts, counts, survive). |
| `crates/bin/parosctl/src/cmd/cell.rs`, `cmd/cell/balance.rs` | Machine admission, drain, removal, maintenance windows, control-plane mode, balance view. |
| `crates/bin/parosctl/src/cmd/universe.rs` | Root keys of the universe, cells, buckets. |
| `crates/bin/parosctl/src/cmd/status.rs` | Status words with hysteresis, usage summed from every machine's `Load`, scrub pass. |
| `crates/bin/parosctl/src/cmd/tls.rs` | Offline CA and leaves; `show` with `x509-parser`; `verify` with rustls verifiers. |
| `crates/bin/parosctl/src/cmd/store.rs` | `store inspect` over `paros::inspect::inspect` with a scratch copy (refused inside the data directory). |
| `crates/bin/parosctl/src/cmd/key.rs`, `cmd/token.rs` | Biscuit key pairs (`.private` mode 0600, written once) and tokens. |
| `crates/bin/parosctl/src/cmd/machine.rs`, `cmd/roles.rs`, `support/view.rs` | `Asker`: one `View` request to the servers' cell, follow `other_cell` to another cell. |
| `crates/bin/parosctl/src/support/labels.rs` | `Labels`: names for every printed entity; short hex only when no view names it. |
| `crates/bin/parosctl/src/support/names.rs` | `JournalRef`, `NodeRef`, `Resolved`: `TENANT/JOURNAL`, `paros://...`, `id:TENANT/JOURNAL` hex prefixes. |
| `crates/bin/parosctl/src/support/cells.rs` | `universe`, `tenant_cell`, `universe_label`: commands that cross cells. |
| `crates/bin/parosctl/src/support/output.rs` | `Printer`, `short`, `table`: text or one JSON document per answer; diagnostics to stderr. |
| `crates/bin/parosctl/src/support/help.rs` | `for_people`: strips `#NNN` and `§N.N` references from every `--help` text. |

## Ideas worth reusing

- **Three-way ending and exit codes** (`Ending`, `support/connect.rs`). Success, refused, ambiguous, unreachable: a script can tell "it did not happen" from "it may have happened". `an_interruption_is_ambiguous_only_when_a_write_may_have_landed` (`cmd/tenant.rs`) holds the rule.
- **The CLI holds no policy** (AGENTS.md). Every retry and claim lives in the client library, so the simulation's operator and the CLI behave the same. The playground's `Operator` copied this shape.
- **Names first, ids last** (`support/labels.rs`, `support/names.rs`, #399). Humans read and type names; a hex prefix must name exactly one candidate (`a_prefix_names_one_candidate_or_is_refused`).
- **Text and JSON from one call** (`Printer::emit` in `support/output.rs`): one closure for text, one for JSON, so they cannot drift.
- **Help without issue numbers** (`support/help.rs`, `for_people`). Developers keep references in doc comments; users never see them. Tested on the whole tree (`every_about_and_help_of_the_tree_loses_its_references`).
- **Offline tools need no cell**: keys, tokens, TLS and store inspection run with no server, so an operator can prepare and debug before anything starts.
- **`store inspect` on a scratch copy** (`cmd/store.rs`): never opens the live data directory for writes; refuses a scratch inside it.
- **Refusal texts start with a stable label** (`every_refusal_starts_with_its_label`) and tell the operator what to do (`an_interrupted_creation_says_how_to_free_its_name`, `each_wait_in_vain_tells_the_operator_what_to_do`).
- **`tls init` prints each process's variables** and never overwrites a file; keys are mode 0600.

Leaks and mistakes not to copy:

- **Policy crept in.** `writes_again` in `src/cmd/data.rs` re-sends a single-writer write at the tail after a stale-position refusal, with the client's retry budget. That is client policy in the CLI, against its own rule.
- **Wire types in the CLI.** `src/cmd/data.rs` imports `paros::wire` protobuf types (`InspectReply`, `RetireRequest`, `WireQuorumSystem`, `journal_state_from_proto`). About 39 `paros::wire` uses across the crate. A CLI should see only client types.
- **Too many nouns for an interim control plane.** `cell add-machine` and `cell drain-machine` claimed the cell control journal directly (see `crates/bin/parosd/AGENTS.md` "Interim"). `docs/architecture` rule 3 (one writer: only the coordinator writes its control journal) makes every admin call a request.
- **`configure` grew layout, counts, survive and durability words** (`cmd/journal.rs`). Rule 7 (the person types what to survive, the cell places) replaces most of it; the tests `colocated_clears_the_held_counts_and_keeps_the_durability` and `survive_reads_machines_and_takes_no_zone_or_region_yet` show the old surface.
- **Size.** 13,397 lines, with `cmd/data.rs` (1,475) and `cmd/journal.rs` (1,333) the largest; much of it is printing and refusal text per outcome enum.
- **A library crate inside a binary crate** (`src/lib.rs`) only so `paros-bench` can reuse support code. A small shared crate would be clearer.

## Bugs and scenarios to cover again

- **Ambiguous vs not written.** A command that was interrupted reports `4` only when a write may have landed; otherwise `5` or `3`. `an_interruption_is_ambiguous_only_when_a_write_may_have_landed`, `each_answer_prints_its_outcome_and_ends_by_it`, `each_ending_has_its_exit_code`.
- **Retry only a stale tail write, within the budget, only as owner, never with `--seq`**: `only_a_stale_position_at_the_tail_is_written_again`. A new client must own this rule.
- **Truncation after a lost term reads as superseded, not refused**: `a_truncation_refused_after_a_lost_term_is_superseded`.
- **Leader uuid is never the unset zero**: `a_leader_uuid_is_1_to_32_hex_digits_and_never_zero`, `a_given_leader_is_kept_and_a_drawn_one_is_never_unset`, `a_drawn_id_is_never_the_unset_zero`.
- **An interrupted tenant creation leaves a `REGISTERING` name**; the text says to delete it: `an_interrupted_creation_says_how_to_free_its_name`.
- **Cross-cell calls name the other cell**: `another_cell_is_named_when_known`, `a_cell_mismatch_names_both_sides`.
- **Through a frontend, a name is sent as text and ids go whole**: `through_a_frontend_a_name_is_sent_as_it_is_and_ids_are_whole`, `a_journal_call_goes_through_the_frontends_when_they_are_given`.
- **A partial TLS setting is refused by its variable**: `a_partial_tls_setting_is_refused_by_its_variable`.
- **The timeout bounds each request and each read**: `the_timeout_bounds_each_request_and_each_read`.
- **Store inspection: an unreadable part refuses the whole disk; an empty disk reads whole**: `an_unreadable_part_makes_the_disk_refused`, `an_empty_disk_holds_no_record_and_reads_whole`, `inspect_reads_an_empty_data_directory_and_refuses_a_scratch_inside_it`.
- **Status hysteresis shows the word judged now while it holds a word back**: `the_word_shows_the_word_judged_now_when_held_back`.
- **TLS round trip and foreign authority**: `init_issue_show_and_verify`, `verify_passes_its_own_authority_and_refuses_another`.
- **Token tools**: `a_minted_token_is_inspected_and_verified_by_its_key`, `a_derived_token_needs_a_restriction_or_a_seal`.
- **The command tree is well formed** (clap debug assert): `the_command_line_is_well_formed`.
- **End-to-end**: `scripts/verify-cell.sh` drove `parosctl` against three local `parosd`; no unit test spawns a process.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/bin/parosctl/src/main.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/bin/parosctl

Where to start reading: `src/main.rs`, `src/support/connect.rs`, `src/cmd/data.rs`, `src/support/names.rs` with `src/support/labels.rs`, then `src/cmd/init.rs`. Its own map at the tag: `crates/bin/parosctl/AGENTS.md`.

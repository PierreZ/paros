+++
title = "Run a simulation seed"
description = "Run one seed of the paros simulation in your browser and see whether every oracle held"
weight = 4
+++

paros is tested in a deterministic simulation. A **seed** is a number. The seed sets every
random choice of one run: how many machines there are, which faults strike and when, and what
the clients do. The same seed always gives the same run. This page runs one seed of the main
simulation campaign in your browser.

{{ sim_seed(seed="1") }}

## What runs

The run is the same campaign that the test suite and the seed hunts run. It is compiled to
WebAssembly, and it does not change for the browser:

- The machines run the same code as `parosd`. They start with empty disks, and the clients
  form a cell with `init`.
- The clients call the journals through the same client library as `parosctl`. They write,
  read, truncate, change the leader, create tenants and journals, and add machines.
- Faults strike during the run. The network drops, delays and reorders messages. Processes
  crash and restart. Disks lose the writes that were not synced. Some machines run slowly.
- Then the faults stop, and the run must recover.

The simulator ([moonpool](https://pierrez.github.io/moonpool/)) gives the code a simulated
network, simulated disks and a simulated clock. These replace the real ones. All times on this
page are simulated milliseconds. They are not production numbers.

## What the result means

The **oracles** are the checks that judge the run. They watch every node and every client
call. For example, one slot of a journal never holds two values, a promise never goes back
after a restart, and every answer a client got fits one order of the calls. "Every oracle
held" means that no check failed in this run. When a check fails, the widget shows its message.

The timeline shows one lane for each process. Each dot is one event that the process logged:
a fault that struck it, a leader it elected, a value that it saw chosen, or a step of the cell's
formation. Select a colour in the legend to hide or show that type of event. Point to a dot to
see its time and fields.

## Replay a seed

The page address keeps the seed, for example `#seed=42`. Open the address again to run the
same seed again. The result and the digest are the same each time. A seed also gives the
same result here as in the native simulation: the CI runs some seeds both ways and compares
every byte.

---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: Quickstart
description: Start a universe of parosd machines with Docker Compose, write and read a journal, kill a machine, and add a second cell.
---

You need Docker with the Compose plugin and nothing else. The Compose file runs with mutual TLS
and a pinned root key. Its keys (`demo/keys/`) are public: use them only for this demo.

## Start the machines

```sh
git clone https://github.com/PierreZ/paros && cd paros
docker compose up -d --build
```

This starts nine machines: `node1` to `node3` (storage, zones a, b and c), `storage4` and
`stateless1` for `cell-1`, and four machines for a second cell. A machine starts idle: it
waits for an operator.

## Form the cell

```sh
docker compose run --rm init
# initialized universe=demo cell=cell-1 coordinator=node1 members=[node1,node2,node3] …
```

`init` is a single-decree Paxos over `node1` to `node3`: they choose the plan of the cell.
Then the cell elects its coordinator and joins the universe.

## A tenant and a journal

```sh
docker compose run --rm parosctl tenant create acme
docker compose run --rm parosctl journal create acme orders
J=acme/orders
```

The tenant coordinator places the journal on three acceptors in three zones. That is the
default durability, `--survive machines=1 --partitions 1`.

## Write and read

```sh
docker compose run --rm parosctl write "$J" hello world --leader 7
docker compose run --rm parosctl read "$J"
docker compose run --rm parosctl read "$J" --from 2 --wait-ms 2000
```

`--leader 7` claims the journal as writer `7`. A second writer takes over with
`SetLeader`, and the first one is refused from then on:

```sh
docker compose run --rm parosctl set-leader "$J" --new 8
docker compose run --rm parosctl write "$J" stale --leader 7 --no-claim   # refused, exit 3
docker compose run --rm parosctl write "$J" fresh --leader 8
```

## Kill a machine

```sh
docker compose kill node2
docker compose run --rm parosctl write "$J" still here --leader 8
docker compose start node2
```

Two acceptors of three are a majority, so writes go on.

## Grow and change the cell

```sh
docker compose run --rm parosctl cell add-machine storage4:4500
docker compose run --rm parosctl cell add-machine stateless1:4500
docker compose run --rm parosctl configure --tenant acme --journal orders --partitions 3 --survive machines=0
docker compose run --rm parosctl cell show
docker compose run --rm parosctl roles --tenant acme
```

`configure` changes the durability of a live journal. The tenant coordinator books new
acceptors, and the leader moves the journal to them by Matchmaker Paxos.

## A second cell

```sh
docker compose run --rm init-cell2
docker compose run --rm add-cell2
docker compose run --rm parosctl tenant create initech --cell cell-2
docker compose run --rm parosctl journal create initech orders
docker compose run --rm parosctl write initech/orders "first order"
```

`init-cell2` forms an idle cell and `add-cell2` joins it to the universe. A command that names a tenant finds its cell through the universe directory. Only commands
about a cell itself take `--servers`.

The full walkthrough is `DEMO.md` in the repository.

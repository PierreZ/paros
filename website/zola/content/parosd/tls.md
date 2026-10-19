+++
title = "TLS"
description = "Encrypt every session and prove every caller with mutual TLS"
weight = 3
+++

Every `parosd` session can run over TLS 1.3. By default the servers also ask every caller
for a certificate: this is mutual TLS. A universe has one certificate authority, and
`parosctl tls` makes every file.

## Make the certificates

One command writes the authority and a certificate for each machine, frontend and operator:

```sh
parosctl tls init --out tls --machines parosd-1,parosd-2,parosd-3 \
  --frontends paros-frontend --universe prod
```

The command prints a table of the files and the variables each process reads. It never
overwrites a file. Each key file has mode 0600: keep it secret.

| command | what it does |
|---|---|
| `parosctl tls init` | writes the authority and every certificate in one step |
| `parosctl tls ca` | writes a new authority only |
| `parosctl tls issue <name> --role <role> --ca-dir tls` | writes one more certificate, for a new machine or a renewal |
| `parosctl tls show <file>` | prints the identity, names, use and validity of each certificate in a file |
| `parosctl tls verify <file> --ca tls/ca.crt` | checks a certificate as a handshake does |

## The certificate layout

Each certificate names its holder with an identity:
`spiffe://<universe>/<role>/<name>`. There are four roles:

- A `machine` (a `parosd`) serves and dials.
- A `frontend` (a `paros-frontend`) serves and dials.
- An `admin` (an operator's `parosctl`) only dials.
- A `client` (an application) only dials.

A machine's or a frontend's certificate also proves the DNS name `paros`. Every client expects
this name. A client dials an address, and an address can change, so the client checks the
authority and the name `paros`, never the address.

## Configure the processes

| process | variables |
|---|---|
| `parosd` | `PAROS_TLS_CERT`, `PAROS_TLS_KEY`, `PAROS_TLS_CA`, `PAROS_TLS_CLIENT_AUTH` |
| `paros-frontend` | `PAROS_FRONTEND_TLS_CERT`, `PAROS_FRONTEND_TLS_KEY`, `PAROS_FRONTEND_TLS_CA`, `PAROS_FRONTEND_TLS_CLIENT_AUTH` |
| `parosctl` | `PAROSCTL_TLS_CA`, `PAROSCTL_TLS_CERT`, `PAROSCTL_TLS_KEY` (or `--tls-ca`, `--tls-cert`, `--tls-key`) |

Each variable names a file. Its `_PEM` twin holds the PEM text itself instead, for a deployment
that passes secrets as variables (`PAROS_TLS_CERT_PEM`, `PAROS_FRONTEND_TLS_KEY_PEM`,
`PAROSCTL_TLS_CA_PEM`, or `--tls-cert-pem`). Give one of the two for each item. A flag wins over
its variable.

A server needs all three files, or none for plaintext. Only some of them is an error, so a typo
never turns TLS off. Every process of a cell must make the same choice: a plaintext process
cannot reach a TLS process.

`*_CLIENT_AUTH` sets what a server asks of a caller:

- `required` (the default): the caller must present a certificate that the authority signed.
- `optional`: the server verifies a certificate when the caller presents one.
- `off`: the server asks for no certificate (server-authenticated TLS).

Mutual TLS decides who can open a session. It does not decide what a caller can do: every
call stays public, and the frontend's tokens still authorize the journal calls.

## Tested in simulation

The deterministic simulation rolls a dice at the start of each seed. On about half of the
seeds, every session runs over TLS with one mode for the seed. Each process issues its
certificate at its boot with the same code as `parosctl tls`.

# The demo key set

The keys `docker-compose.yml` passes to the demo, as text in variables. They are public: never
use them for anything but the demo. Each file is also inline in `docker-compose.yml`.

| file | what it is | where the demo passes it |
|---|---|---|
| `ca.crt`, `ca.key` | the authority of the universe `demo` | `PAROS_TLS_CA_PEM`, `PAROSCTL_TLS_CA_PEM` |
| `machine.crt`, `machine.key` | one machine certificate, shared by the five machines | `PAROS_TLS_CERT_PEM`, `PAROS_TLS_KEY_PEM` |
| `admin.crt`, `admin.key` | the operator's client certificate | `PAROSCTL_TLS_CERT_PEM`, `PAROSCTL_TLS_KEY_PEM` |
| `demo.public`, `demo.private` | the root key pair | `PAROS_ROOT_PUBLIC_KEY_JSON`, `PAROSCTL_ROOT_PUBLIC_KEY_JSON` |
| `admin.token` | an `admin` token the root key signed, valid ten years | `PAROS_TOKEN` |

To mint a tenant token for the frontends, sign it with the demo key:

```sh
parosctl token mint --key demo/keys/demo.private --role tenant --tenant acme --ttl 7d --subject demo-app
```

To make a new set, run these commands, then copy each file into `docker-compose.yml`:

```sh
parosctl tls init --out demo/keys --machines machine --admins admin --universe demo --days 3650 --ca-days 3660
parosctl key generate --out demo/keys --label demo
parosctl token mint --key demo/keys/demo.private --role admin --ttl 3650d --subject demo-operator --out demo/keys/admin.token
```

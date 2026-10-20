# SMPP hub

[![CI](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml)
[![smpp-core](https://img.shields.io/crates/v/smpp-core.svg?label=smpp-core)](https://crates.io/crates/smpp-core)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/chrismoos/smpp-hub/blob/main/LICENSE)

Route SMS between networks with a standalone SMPP server, or embed the hub in an application with its own authentication and storage.

This workspace contains two crates:

- `smpp-core`: SMPP v3.4 encoding, sessions, text handling, delivery receipts, and Tokio connections.
- `smpp-hub`: authenticated number routing, durable store-and-forward delivery, retries, expiry, and receipt forwarding.

## Run the server

Copy `examples/hub.json` to `hub.local.json`. Set a separate password of at most 8 ASCII characters for each peer and list the numbers it owns. Then run:

```sh
cargo run --release -p smpp-hub -- --config hub.local.json
```

The example listens on localhost port 2775 and routes between numbers 1000 and 1001. Network A connects to the hub. The hub connects to Network B on localhost port 2776. Both use their configured `smpp_system_id` and password. Keep peer IDs stable across restarts. Each peer may have one active session.

Pending messages are stored in SQLite and survive restarts. The hub removes their content after handoff, failure, or expiry. Handset delivery reports are forwarded when supported and requested.

Configuration is read at startup.

## Number routing

Each peer lists the numbers it owns in `numbers`. An entry made only of digits is an exact number. Any other entry is a regular expression that must match the whole number, so `70255501\d\d` matches `7025550123` but not `17025550123`.

The hub picks a destination in this order:

1. An exact number. Each exact number may be listed for only one peer.
2. Otherwise, the first pattern that matches, in the order the peers and their entries appear in the file.

A peer may send only from numbers it owns under the same rules, and the hub never delivers a message back to the peer that sent it. Reserved numbers are refused before any route is considered. A pattern that does not compile stops the hub at startup with the peer and pattern named.

Patterns let a peer own a range without listing every number:

```json
"numbers": ["7025550100", "70255502\\d{2}", "4155550(1|2)\\d\\d"]
```

JSON needs each backslash written twice.

### Several networks behind one upstream connection

A catch-all `.*` sends every number that no other peer owns to one peer. With that peer pointed at a larger hub, several local networks share a single upstream account. Messages between local networks stay local, and everything else goes upstream:

```json
"peers": [
  { "name": "Network A", "smpp_system_id": "net-a", "bind_role": "site_binds_to_hub",
    "password": "exampleA", "numbers": ["1000", "1001"], "id": "…" },
  { "name": "Network B", "smpp_system_id": "net-b", "bind_role": "site_binds_to_hub",
    "password": "exampleB", "numbers": ["100[2-5]"], "id": "…" },
  { "name": "Upstream hub", "smpp_system_id": "upstream-id", "bind_role": "hub_binds_to_site",
    "address": "100.71.0.1:2775", "password": "exampleU", "messages_per_hour": 3000,
    "numbers": [".*"], "id": "…" }
]
```

`examples/upstream.json` is a complete version.

- Use the login the upstream hub gave you for the upstream peer.
- The upstream hub must route all your local numbers to that login.
- Messages from upstream for numbers no local network owns are refused, not sent back.
- Give the upstream peer a higher `messages_per_hour`, since it carries everyone's traffic.

| Option | What it sets |
|---|---|
| Peer `numbers` | Exact numbers and whole-number patterns the peer owns. See [Number routing](#number-routing). |
| Peer `bind_role` | `site_binds_to_hub` for peers that connect to the hub, or `hub_binds_to_site` for peers the hub connects to. |
| Peer `address` | The peer's IP address and SMPP port when the hub connects to it. |
| `reserved_numbers` | Numbers the hub must not route. |
| `quotas.message_ttl_hours` | How long a pending message is kept. Default: 24 hours. |
| `quotas.messages_per_site_hour` | Default sending limit for each peer. Default: 600 per hour. |
| Peer `messages_per_hour` | Overrides the sending limit for that peer. |

## Validate

```sh
cargo fmt --all -- --check
cargo clippy --release --workspace --all-targets -- -D warnings
cargo test --release --workspace --all-targets
```

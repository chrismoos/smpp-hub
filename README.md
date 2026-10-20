# SMPP hub

[![CI](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml)
[![smpp-core](https://img.shields.io/crates/v/smpp-core.svg?label=smpp-core)](https://crates.io/crates/smpp-core)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

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

| Option | What it sets |
|---|---|
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

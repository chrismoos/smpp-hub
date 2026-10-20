# smpp-core

[![CI](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/chrismoos/smpp-hub/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Rust library for sending and receiving SMS over SMPP v3.4. Use it to connect to a message center or accept connections from SMS applications.

The library handles connections, binding, keepalives, message responses, timeouts, and graceful disconnects. It also supports multipart messages, Unicode text, and delivery receipts. Your application supplies authentication, routing, storage, and retry policy.

## Add the dependency

```toml
[dependencies]
smpp-core = "0.1.0"
```

## Try it

Run the [echo server](examples/server.rs) in one terminal:

```sh
cargo run --release --example server
```

Then run the [client](examples/client.rs) in another:

```sh
cargo run --release --example client -- 127.0.0.1:2775 "Hello SMPP"
```

The client connects, submits your message, receives an echo, and disconnects. The server accepts multiple clients. Press Ctrl+C in the server terminal to close its connections and stop.

Both examples use the system ID `example` and password `password`. The server keeps up to 100 accepted messages in memory per connection and refuses more when full. Replace that with durable storage before acknowledging messages in your own application. To use another address, pass it as the first argument to either example.

## Integrate it

Start with `Connection`, `Config`, and `MessageBody`, as shown in the examples. Handle bind requests with your authentication logic, store incoming messages before acknowledging them, and record the results of outgoing submissions. A successful submission response means the peer accepted the message. Delivery to a handset requires a delivery receipt.

`ConnectionOptions` controls bind, write, and shutdown deadlines. Call `shutdown` before stopping the Tokio runtime, then drain remaining events to record late responses. Reconnects do not automatically resend messages. If a connection ends before a response arrives, the message's outcome may be unknown.

For more control, `Session` provides the protocol state machine, while `PduReader` and `PduWriter` handle individual messages on an async stream. Optional fields preserve native IS-637 bearer data for CDMA applications.

## Checks

```sh
cargo fmt --all --check
cargo test --release
cargo clippy --release --workspace --all-targets -- -D warnings
```

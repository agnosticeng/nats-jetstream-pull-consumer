# nats-jetstream-pull-consumer

A pull consumer for [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) with pluggable interceptors, error strategies, and support for both batch and stream processing modes.

## Overview

This crate provides a reusable pull consumer framework for NATS JetStream. It handles connection management, message polling, and ack/nack lifecycle so you can focus on your business logic.

### Key features

- **Batch processing** — Poll batches of messages on a fixed interval and process them together
- **Stream processing** — Concurrent message processing with automatic progress ack extension
- **Pluggable interceptors** — Chain interceptors that transform or validate messages before they reach the handler
- **Error strategies** — Choose between `Stop` (halt on error) or `Log` (log and continue)
- **Graceful shutdown** — Uses `CancellationToken` for clean shutdown
- **Built-in decompression** — Ships with a `DecompressInterceptor` that handles raw and LZ4-compressed payloads

## Quick start

```rust
use nats_jetstream_pull_consumer::{
    DecompressInterceptor, ErrorStrategy, Handler, NatsConfig, Runner, RunnerConfig,
};
use tokio_util::sync::CancellationToken;

// Configure the NATS connection and consumer
let nats = NatsConfig {
    endpoint: "nats://localhost:4222".into(),
    stream: "my_stream".into(),
    consumer: "my_consumer".into(),
};

// Configure the runner
let conf = RunnerConfig {
    error_strategy: ErrorStrategy::Log,
    batch_size: 10,
    flush_interval_ms: 1000,
    ..Default::default()
};

// Build the runner with decompression interceptor
let runner = Runner::new(conf).with_interceptor(DecompressInterceptor::new());

// Pick your handler and run
// runner.run(&nats, handler, CancellationToken::new()).await?;
```

## Processing modes

### Batch handler

Implement `BatchHandler` to receive batches of messages:

```rust
use async_trait::async_trait;
use nats_jetstream_pull_consumer::BatchHandler;

struct MyBatchHandler;

#[async_trait]
impl BatchHandler for MyBatchHandler {
    fn name(&self) -> &str {
        "my-batch-handler"
    }

    async fn process_batch(&self, msgs: Vec<async_nats::jetstream::Message>) -> Result<(), anyhow::Error> {
        for msg in &msgs {
            println!("received: {:?}", msg.message.payload.len());
        }
        Ok(())
    }
}
```

### Stream handler

Implement `StreamHandler` for concurrent per-message processing with automatic progress ack extension:

```rust
use async_trait::async_trait;
use nats_jetstream_pull_consumer::StreamHandler;
use std::sync::Arc;

struct MyStreamHandler;

#[async_trait]
impl StreamHandler for MyStreamHandler {
    fn name(&self) -> &str {
        "my-stream-handler"
    }

    async fn process_message(&self, msg: &async_nats::jetstream::Message) -> Result<(), anyhow::Error> {
        println!("processing message: {:?}", msg.message.payload.len());
        Ok(())
    }
}
```

## Interceptors

Interceptors transform or validate messages before they reach handlers. Implement the `Interceptor` trait:

```rust
use async_trait::async_trait;
use nats_jetstream_pull_consumer::Interceptor;

struct LoggingInterceptor;

#[async_trait]
impl Interceptor for LoggingInterceptor {
    async fn intercept(&self, msg: &mut async_nats::jetstream::Message) -> Result<(), anyhow::Error> {
        tracing::info!("message size: {}", msg.message.payload.len());
        Ok(())
    }
}
```

### DecompressInterceptor

The crate includes a built-in `DecompressInterceptor` that handles a 1-byte codec tag prefix:

| Tag | Meaning |
|-----|---------|
| `0x00` | Raw — tag is stripped, payload is passed through |
| `0x01` | LZ4 compressed — tag is stripped, payload is decompressed |

## Error strategies

- **`ErrorStrategy::Stop`** — Logs the error and stops the worker, returning an `Error::Stopped`
- **`ErrorStrategy::Log`** — Logs the error and continues processing

## License

Licensed under either of:

- MIT license ([LICENSE](LICENSE) or https://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0)

at your option.
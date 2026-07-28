use crate::NatsConfig;
use crate::error::{Error, Result};
use crate::error_strategy::ErrorStrategy;
use crate::handler::{BatchHandler, Handler, StreamHandler};
use crate::interceptor::{Interceptor, InterceptorChain};
use async_nats::jetstream::{AckKind, consumer::PullConsumer};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RunnerConfig {
    pub error_strategy: ErrorStrategy,
    pub max_concurrency: usize,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub ack_extension_interval_ms: u64,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            error_strategy: ErrorStrategy::Log,
            max_concurrency: 4,
            batch_size: 10,
            flush_interval_ms: 1000,
            ack_extension_interval_ms: 10_000,
        }
    }
}

pub struct Runner {
    conf: RunnerConfig,
    chain: InterceptorChain,
}

impl Runner {
    pub fn new(conf: RunnerConfig) -> Self {
        Self {
            conf,
            chain: InterceptorChain::new(),
        }
    }

    pub fn with_interceptor(mut self, interceptor: impl Interceptor + 'static) -> Self {
        self.chain.push(Box::new(interceptor));
        self
    }

    pub async fn run(
        &self,
        nats: &NatsConfig,
        handler: Handler,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let client = async_nats::connect(&nats.endpoint)
            .await
            .map_err(|e| Error::Nats(e.to_string()))?;
        let js = async_nats::jetstream::new(client);
        let stream = js
            .get_stream(&nats.stream)
            .await
            .map_err(|e| Error::Nats(e.to_string()))?;
        let consumer: PullConsumer = stream
            .get_consumer(&nats.consumer)
            .await
            .map_err(|e| Error::Nats(e.to_string()))?;

        self.run_with_consumer(consumer, handler, cancel).await
    }

    pub async fn run_with_consumer(
        &self,
        consumer: PullConsumer,
        handler: Handler,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        match handler {
            Handler::Batch(h) => self.run_batch(consumer, h, cancel).await,
            Handler::Stream(h) => self.run_stream(consumer, h, cancel).await,
        }
    }

    async fn run_batch(
        &self,
        consumer: PullConsumer,
        handler: Box<dyn BatchHandler>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let handler = Arc::new(handler);
        let flush_interval = Duration::from_millis(self.conf.flush_interval_ms);

        loop {
            let msgs = tokio::select! {
                _ = cancel.cancelled() => break,
                result = consumer
                    .batch()
                    .max_messages(self.conf.batch_size)
                    .expires(flush_interval)
                    .messages() => {
                    let batch = result?;
                    batch
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| Error::BatchStream(e.to_string()))?
                }
            };

            if msgs.is_empty() {
                continue;
            }

            let mut decoded = Vec::with_capacity(msgs.len());
            for mut msg in msgs {
                self.chain.apply(&mut msg).await?;
                decoded.push(msg);
            }

            match handler.process_batch(decoded).await {
                Ok(()) => {}
                Err(e) => {
                    if self.conf.error_strategy.process_error(e).is_err() {
                        return Err(Error::Stopped(
                            "handler error; error strategy is Stop".into(),
                        ));
                    }
                }
            }
        }

        tracing::info!("worker shutdown complete");
        Ok(())
    }

    async fn run_stream(
        &self,
        consumer: PullConsumer,
        handler: Box<dyn StreamHandler>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let handler = Arc::new(handler);
        let messages = consumer
            .stream()
            .max_messages_per_batch(self.conf.max_concurrency)
            .messages()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        let ack_extension_interval = Duration::from_millis(self.conf.ack_extension_interval_ms);

        tokio::select! {
            _ = cancel.cancelled() => {}
            res = messages
                .map_err(|e| Error::Stream(e.to_string()))
                .try_for_each_concurrent(self.conf.max_concurrency, |msg| {
                    let handler = handler.clone();
                    let error_strategy = self.conf.error_strategy.clone();
                    async move {
                        let msg = Arc::new(msg);

                        let progress_cancel = CancellationToken::new();
                        let progress_cancel_clone = progress_cancel.clone();
                        let msg_for_progress = msg.clone();

                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = progress_cancel_clone.cancelled() => break,
                                    _ = tokio::time::sleep(ack_extension_interval) => {
                                        if let Err(e) = msg_for_progress.ack_with(AckKind::Progress).await {
                                            tracing::warn!("failed to send progress ack: {}", e);
                                        }
                                    }
                                }
                            }
                        });

                        let result = handler.process_message(&msg).await;
                        progress_cancel.cancel();

                        match result {
                            Ok(()) => {
                                if let Err(e) = msg.ack().await {
                                    tracing::error!("failed to ack message: {}", e);
                                }
                            }
                            Err(e) => {
                                if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                    tracing::error!("failed to nack message: {}", e);
                                }
                                return error_strategy.process_error(e).map_err(|e| Error::Stopped(e.to_string()))
                            }
                        }

                        Ok(())
                    }
                }) => { res? }
        }

        tracing::info!("worker shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_config_defaults() {
        let conf = RunnerConfig::default();
        assert_eq!(conf.max_concurrency, 4);
        assert_eq!(conf.batch_size, 10);
        assert_eq!(conf.flush_interval_ms, 1000);
        assert_eq!(conf.ack_extension_interval_ms, 10_000);
        assert!(matches!(conf.error_strategy, ErrorStrategy::Log));
    }

    #[test]
    fn test_runner_new() {
        let conf = RunnerConfig::default();
        let runner = Runner::new(conf);
        assert!(runner.chain.is_empty());
    }
}

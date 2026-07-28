#![forbid(unsafe_code)]

pub mod decompress_interceptor;
pub mod error;
pub mod error_strategy;
pub mod handler;
pub mod interceptor;
pub mod runner;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsConfig {
    pub endpoint: String,
    pub stream: String,
    pub consumer: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            endpoint: "nats://localhost:4222".to_string(),
            stream: String::new(),
            consumer: String::new(),
        }
    }
}

pub use decompress_interceptor::DecompressInterceptor;
pub use error::{Error, Result};
pub use error_strategy::ErrorStrategy;
pub use handler::{BatchHandler, Handler, StreamHandler};
pub use interceptor::{Interceptor, InterceptorChain};
pub use runner::{Runner, RunnerConfig};

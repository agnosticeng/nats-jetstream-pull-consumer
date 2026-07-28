use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    Stop,
    #[default]
    Log,
}

impl ErrorStrategy {
    pub fn process_error(&self, e: anyhow::Error) -> anyhow::Result<()> {
        match self {
            ErrorStrategy::Stop => {
                tracing::error!("Worker processing error: {}", e);
                Err(e)
            }
            ErrorStrategy::Log => {
                tracing::error!("Worker processing error (continuing): {}", e);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_log() {
        assert!(matches!(ErrorStrategy::default(), ErrorStrategy::Log));
    }

    #[test]
    fn test_stop_returns_error() {
        let strategy = ErrorStrategy::Stop;
        let result = strategy.process_error(anyhow::anyhow!("test error"));
        assert!(result.is_err());
    }

    #[test]
    fn test_log_returns_ok() {
        let strategy = ErrorStrategy::Log;
        let result = strategy.process_error(anyhow::anyhow!("test error"));
        assert!(result.is_ok());
    }
}

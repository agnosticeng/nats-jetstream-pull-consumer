#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("nats: {0}")]
    Nats(String),

    #[error("nats batch fetch: {0}")]
    BatchFetch(#[from] async_nats::jetstream::consumer::pull::BatchError),

    #[error("nats batch stream: {0}")]
    BatchStream(String),

    #[error("nats stream: {0}")]
    Stream(String),

    #[error("interceptor: {0}")]
    Interceptor(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("worker stopped: {0}")]
    Stopped(String),
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Interceptor(e.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_nats() {
        let err = Error::Nats("connection refused".into());
        assert_eq!(err.to_string(), "nats: connection refused");
    }

    #[test]
    fn test_error_display_batch_stream() {
        let err = Error::BatchStream("timeout".into());
        assert_eq!(err.to_string(), "nats batch stream: timeout");
    }

    #[test]
    fn test_error_display_stream() {
        let err = Error::Stream("disconnected".into());
        assert_eq!(err.to_string(), "nats stream: disconnected");
    }

    #[test]
    fn test_error_display_stopped() {
        let err = Error::Stopped("shutdown".into());
        assert_eq!(err.to_string(), "worker stopped: shutdown");
    }

    #[test]
    fn test_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something went wrong");
        let err: Error = anyhow_err.into();
        assert!(matches!(err, Error::Interceptor(_)));
    }
}

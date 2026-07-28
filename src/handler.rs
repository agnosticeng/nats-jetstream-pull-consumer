use async_nats::jetstream::Message;
use async_trait::async_trait;

#[async_trait]
pub trait BatchHandler: Send + Sync {
    fn name(&self) -> &str;
    async fn process_batch(&self, msgs: Vec<Message>) -> Result<(), anyhow::Error>;
}

#[async_trait]
pub trait StreamHandler: Send + Sync {
    fn name(&self) -> &str;
    async fn process_message(&self, msg: &Message) -> Result<(), anyhow::Error>;
}

pub enum Handler {
    Batch(Box<dyn BatchHandler>),
    Stream(Box<dyn StreamHandler>),
}

impl Handler {
    pub fn name(&self) -> String {
        match self {
            Handler::Batch(h) => h.name().to_owned(),
            Handler::Stream(h) => h.name().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBatchHandler;

    #[async_trait]
    impl BatchHandler for TestBatchHandler {
        fn name(&self) -> &str {
            "test-batch"
        }
        async fn process_batch(&self, _msgs: Vec<Message>) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    struct TestStreamHandler;

    #[async_trait]
    impl StreamHandler for TestStreamHandler {
        fn name(&self) -> &str {
            "test-stream"
        }
        async fn process_message(&self, _msg: &Message) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[test]
    fn test_handler_name_batch() {
        let handler = Handler::Batch(Box::new(TestBatchHandler));
        assert_eq!(handler.name(), "test-batch");
    }

    #[test]
    fn test_handler_name_stream() {
        let handler = Handler::Stream(Box::new(TestStreamHandler));
        assert_eq!(handler.name(), "test-stream");
    }
}

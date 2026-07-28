use async_nats::jetstream::Message;
use async_trait::async_trait;

#[async_trait]
pub trait Interceptor: Send + Sync {
    async fn intercept(&self, msg: &mut Message) -> Result<(), anyhow::Error>;
}

pub struct InterceptorChain {
    interceptors: Vec<Box<dyn Interceptor>>,
}

impl InterceptorChain {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn push(&mut self, interceptor: Box<dyn Interceptor>) {
        self.interceptors.push(interceptor);
    }

    pub async fn apply(&self, msg: &mut Message) -> Result<(), anyhow::Error> {
        for interceptor in &self.interceptors {
            interceptor.intercept(msg).await?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.interceptors.len()
    }
}

impl Default for InterceptorChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_chain_is_empty() {
        let chain = InterceptorChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
    }

    #[test]
    fn test_push_increases_len() {
        struct DummyInterceptor;

        #[async_trait]
        impl Interceptor for DummyInterceptor {
            async fn intercept(&self, _msg: &mut Message) -> Result<(), anyhow::Error> {
                Ok(())
            }
        }

        let mut chain = InterceptorChain::new();
        chain.push(Box::new(DummyInterceptor));
        assert_eq!(chain.len(), 1);
        chain.push(Box::new(DummyInterceptor));
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_default_is_empty() {
        let chain = InterceptorChain::default();
        assert!(chain.is_empty());
    }
}

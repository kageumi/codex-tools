use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Default)]
pub(crate) struct ActivationLock(
    pub(crate) tokio::sync::Mutex<()>,
    AtomicU64,
    /// 串行化会读取 `/models` 并提交 Provider/cache/config 的完整事务。
    /// active save 会先持久化中间态，必须阻止 refresh/activate 消费它。
    pub(crate) tokio::sync::Mutex<()>,
);

impl ActivationLock {
    /// 在任何锁外等待前登记激活意图。后开始的激活立即使旧 operation 失效，
    /// 即使旧请求更晚才取得互斥锁，也不能覆盖用户最后一次选择。
    pub(crate) fn begin_operation(&self) -> u64 {
        self.1.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }

    pub(crate) fn is_current(&self, operation: u64) -> bool {
        self.1.load(Ordering::Acquire) == operation
    }
}

#[derive(Default)]
pub(crate) struct ApiClient(
    pub(crate) crate::network::ClientCache,
    pub(crate) tokio::sync::Mutex<()>,
);

impl ApiClient {
    pub(crate) fn current(&self) -> Result<reqwest::Client, crate::AppError> {
        self.0
            .current(|builder| {
                builder
                    .redirect(reqwest::redirect::Policy::none())
                    .connect_timeout(Duration::from_secs(30))
                    .timeout(Duration::from_secs(60))
                    .pool_max_idle_per_host(4)
                    .pool_idle_timeout(Duration::from_secs(90))
                    .tcp_keepalive(Duration::from_secs(60))
                    .build()
            })
            .map_err(|error| {
                crate::AppError::Internal(format!("无法初始化网络客户端：{}", error.without_url()))
            })
    }

    pub(crate) fn invalidate(&self) {
        self.0.invalidate();
    }
}

/// 同一 OpenAI 账号的重置卡消费必须串行，避免两个窗口把同一张卡并发提交。
#[derive(Default)]
pub(crate) struct ResetCreditOperations(
    tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    tokio::sync::Mutex<HashSet<(String, String)>>,
);

impl ResetCreditOperations {
    pub(crate) async fn lock(&self, account_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.0.lock().await;
            locks.entry(account_id.to_owned()).or_default().clone()
        };
        lock.lock_owned().await
    }

    /// 仅结果未知的卡被锁定；已知的 no_credit/nothing_to_reset 允许以后经用户明确操作再次检查。
    pub(crate) async fn is_unknown(&self, account_id: &str, credit_id: &str) -> bool {
        self.1
            .lock()
            .await
            .contains(&(account_id.to_owned(), credit_id.to_owned()))
    }

    pub(crate) async fn mark_unknown(&self, account_id: &str, credit_id: &str) {
        self.1
            .lock()
            .await
            .insert((account_id.to_owned(), credit_id.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn newer_activation_operation_invalidates_an_older_one() {
        let activation = ActivationLock::default();
        let older = activation.begin_operation();
        assert!(activation.is_current(older));

        let newer = activation.begin_operation();
        assert!(!activation.is_current(older));
        assert!(activation.is_current(newer));
    }

    #[tokio::test]
    async fn provider_model_transactions_do_not_overlap() {
        let transactions = Arc::new(ActivationLock::default());
        let first = transactions.2.lock().await;
        let waiting = transactions.clone();
        let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let second = tokio::spawn(async move {
            let _guard = waiting.2.lock().await;
            entered.send(()).unwrap();
        });

        tokio::task::yield_now().await;
        assert!(observed.try_recv().is_err());
        drop(first);
        second.await.unwrap();
        assert_eq!(observed.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn queued_activation_is_stale_before_network_work_begins() {
        let activation = Arc::new(ActivationLock::default());
        let transaction = activation.2.lock().await;
        let older = activation.begin_operation();
        let waiting = activation.clone();
        let queued = tokio::spawn(async move {
            let _guard = waiting.2.lock().await;
            waiting.is_current(older)
        });

        tokio::task::yield_now().await;
        let newer = activation.begin_operation();
        drop(transaction);

        assert!(!queued.await.unwrap());
        assert!(activation.is_current(newer));
    }

    #[tokio::test]
    async fn only_unknown_reset_credit_operations_remain_blocked() {
        let operations = ResetCreditOperations::default();
        assert!(!operations.is_unknown("account", "credit").await);
        operations.mark_unknown("account", "credit").await;
        assert!(operations.is_unknown("account", "credit").await);
        assert!(!operations.is_unknown("account", "other-credit").await);
    }
}

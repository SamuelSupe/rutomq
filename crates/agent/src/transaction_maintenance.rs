use crate::health::Metrics;
use rutomq_control::MetadataStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const SWEEP_LIMIT: usize = 1_000;

pub fn spawn(metadata: Arc<dyn MetadataStore>, metrics: Arc<Metrics>, cleanup_interval: Duration) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            match metadata.abort_expired_transactions_batch(SWEEP_LIMIT).await {
                Ok(expired) if expired > 0 => {
                    metrics.expired_transactions.inc_by(expired);
                }
                Ok(_) => {}
                Err(error) => {
                    metrics.transaction_maintenance_errors.inc();
                    warn!(%error, "failed to abort expired transactions");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_control::{MemoryMetadataStore, PartitionKey};

    #[tokio::test(start_paused = true)]
    async fn waits_for_the_configured_interval_before_aborting_timed_out_transactions() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata.create_topic("transactions", 1).await.unwrap();
        let producer = metadata
            .init_producer(Some("timed-out"), 1, None)
            .await
            .unwrap();
        metadata
            .add_partitions_to_transaction(
                "timed-out",
                producer,
                &[PartitionKey::new("transactions", 0)],
                false,
            )
            .await
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));

        let metrics = Arc::new(Metrics::new().unwrap());
        spawn(metadata, metrics.clone(), Duration::from_millis(10_000));
        tokio::task::yield_now().await;
        assert_eq!(metrics.expired_transactions.get(), 0);

        tokio::time::advance(Duration::from_millis(9_999)).await;
        tokio::task::yield_now().await;
        assert_eq!(metrics.expired_transactions.get(), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(metrics.expired_transactions.get(), 1);
    }
}

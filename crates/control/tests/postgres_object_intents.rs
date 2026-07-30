use rutomq_control::{
    BatchDraft, ControlError, MetadataStore, ObjectRef, PartitionKey, PostgresMetadataStore,
    ProducerBatch,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Barrier;
use uuid::Uuid;

// A fixed historical cutoff keeps parallel tests from claiming freshly staged objects.
const AGED_INTENT_CUTOFF_MS: i64 = 978_307_200_000;

#[tokio::test]
async fn postgres_upload_intents_serialize_commit_and_orphan_claim() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("object-intent-topic-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();

    let committed_object = object(&suffix, "committed");
    store.stage_object(committed_object.clone()).await.unwrap();
    assert!(store.object_staged(&committed_object.key).await.unwrap());
    store
        .commit_object(committed_object.clone(), vec![draft(partition.clone())])
        .await
        .unwrap();
    assert!(store.object_committed(&committed_object.key).await.unwrap());
    assert!(!store.object_staged(&committed_object.key).await.unwrap());

    let unstaged = object(&suffix, "unstaged");
    assert!(matches!(
        store
            .commit_object(unstaged, vec![draft(partition.clone())])
            .await,
        Err(ControlError::InvalidRequest(_))
    ));

    let expired = object(&suffix, "expired");
    store.stage_object(expired.clone()).await.unwrap();
    age_intent(&pool, &expired.key).await;
    let claimed = store
        .claim_stale_objects(AGED_INTENT_CUTOFF_MS, 1_000)
        .await
        .unwrap();
    assert!(claimed.contains(&expired.key));
    assert!(store.object_staged(&expired.key).await.unwrap());
    assert!(matches!(
        store
            .commit_object(expired.clone(), vec![draft(partition.clone())])
            .await,
        Err(ControlError::InvalidRequest(_))
    ));
    assert!(
        store
            .complete_stale_object_deletion(&expired.key)
            .await
            .unwrap()
    );
    assert!(!store.object_staged(&expired.key).await.unwrap());
    assert!(
        !store
            .complete_stale_object_deletion(&expired.key)
            .await
            .unwrap()
    );

    let raced = object(&suffix, "raced");
    store.stage_object(raced.clone()).await.unwrap();
    age_intent(&pool, &raced.key).await;
    let barrier = Arc::new(Barrier::new(2));
    let commit_store = store.clone();
    let commit_object = raced.clone();
    let commit_partition = partition.clone();
    let commit_barrier = barrier.clone();
    let commit = tokio::spawn(async move {
        commit_barrier.wait().await;
        commit_store
            .commit_object(commit_object, vec![draft(commit_partition)])
            .await
    });
    let claim_store = store.clone();
    let claim_barrier = barrier.clone();
    let claim = tokio::spawn(async move {
        claim_barrier.wait().await;
        claim_store
            .claim_stale_objects(AGED_INTENT_CUTOFF_MS, 1_000)
            .await
    });
    let committed = commit.await.unwrap().is_ok();
    let claimed = claim.await.unwrap().unwrap().contains(&raced.key);
    assert_ne!(committed, claimed);
    assert_eq!(store.object_committed(&raced.key).await.unwrap(), committed);
    if claimed {
        assert!(store.object_staged(&raced.key).await.unwrap());
        assert!(
            store
                .complete_stale_object_deletion(&raced.key)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn postgres_keeps_duplicate_produce_upload_visible_to_orphan_gc() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("duplicate-object-intent-{suffix}");
    let partition = PartitionKey::new(&topic, 0);
    store.create_topic(&topic, 1).await.unwrap();
    let producer = store.init_producer(None, 60_000, None).await.unwrap();
    let draft = BatchDraft {
        partition,
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: 0,
            last_sequence: 0,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };

    let first = object(&suffix, "idempotent-first");
    store.stage_object(first.clone()).await.unwrap();
    let first_span = store
        .commit_object(first, vec![draft.clone()])
        .await
        .unwrap();

    let retry = object(&suffix, "idempotent-retry");
    store.stage_object(retry.clone()).await.unwrap();
    let retry_span = store
        .commit_object(retry.clone(), vec![draft])
        .await
        .unwrap();
    assert_eq!(retry_span[0].base_offset, first_span[0].base_offset);
    assert!(!store.object_committed(&retry.key).await.unwrap());
    assert!(store.object_staged(&retry.key).await.unwrap());

    age_intent(&pool, &retry.key).await;
    let claimed = store
        .claim_stale_objects(AGED_INTENT_CUTOFF_MS, 1_000)
        .await
        .unwrap();
    assert!(claimed.contains(&retry.key));
    assert!(store.object_staged(&retry.key).await.unwrap());
    assert!(
        store
            .complete_stale_object_deletion(&retry.key)
            .await
            .unwrap()
    );
    assert!(!store.object_staged(&retry.key).await.unwrap());
}

fn object(suffix: &str, label: &str) -> ObjectRef {
    ObjectRef {
        key: format!("objects/{suffix}-{label}"),
        size: 8,
    }
}

fn draft(partition: PartitionKey) -> BatchDraft {
    BatchDraft {
        partition,
        byte_start: 0,
        byte_end: 8,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: None,
        transactional_id: None,
        verify_transaction_partition: true,
    }
}

async fn age_intent(pool: &PgPool, key: &str) {
    sqlx::query(
        "UPDATE objects
         SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'
         WHERE object_key = $1",
    )
    .bind(key)
    .execute(pool)
    .await
    .unwrap();
}

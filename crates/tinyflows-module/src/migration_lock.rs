//! Ephemeral serialization for migrations sharing one scoped callback.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use tokio::sync::{Mutex, OwnedMutexGuard};

type LockMap = StdMutex<HashMap<String, Weak<Mutex<()>>>>;
static LOCKS: OnceLock<LockMap> = OnceLock::new();

/// Serialize migration calls for one host callback without retaining its
/// context or keeping inactive scope locks alive.
pub(super) async fn lock(key: String) -> OwnedMutexGuard<()> {
    lock_for(&key).lock_owned().await
}

fn lock_for(key: &str) -> Arc<Mutex<()>> {
    let locks = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key.to_owned(), Arc::downgrade(&lock));
    lock
}

#[cfg(test)]
#[path = "migration_lock_tests.rs"]
mod tests;

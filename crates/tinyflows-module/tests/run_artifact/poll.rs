use super::*;

pub(super) async fn poll_until_kind(
    proxy: &tinybus::Proxy,
    storage: &CatalogStorage,
    run_id: &str,
    kind: &str,
) -> Vec<runs::RunEvent> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut cursor = 0;
        let mut all = Vec::new();
        loop {
            let page: runs::PollRun = proxy
                .call(
                    "PollRun",
                    (RunControlRequest {
                        storage: storage.clone(),
                        run_id: run_id.into(),
                        cursor,
                    },),
                )
                .await
                .unwrap();
            cursor = page.next_cursor;
            all.extend(page.events);
            if all.iter().any(|event| event.kind == kind) {
                return all;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("requested run event should reach PollRun")
}

pub(super) async fn poll_until_done(
    proxy: &tinybus::Proxy,
    storage: &CatalogStorage,
    run_id: &str,
) -> runs::PollRun {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut cursor = 0;
        let mut all = Vec::new();
        loop {
            let page: runs::PollRun = proxy
                .call(
                    "PollRun",
                    (RunControlRequest {
                        storage: storage.clone(),
                        run_id: run_id.into(),
                        cursor,
                    },),
                )
                .await
                .unwrap();
            cursor = page.next_cursor;
            all.extend(page.events);
            if page.done {
                return runs::PollRun {
                    events: all,
                    next_cursor: cursor,
                    done: true,
                };
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("run actor should flush terminal events")
}

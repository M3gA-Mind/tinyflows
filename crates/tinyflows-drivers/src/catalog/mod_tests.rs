use super::*;

#[test]
fn instants_are_epoch_milliseconds_and_render_back() {
    assert_eq!(instant_ms("2026-01-01T00:00:00Z"), 1_767_225_600_000);
    assert_eq!(
        instant_ms("2026-01-01T01:00:00+02:00"),
        instant_ms("2025-12-31T23:00:00Z"),
        "offsets compare as instants"
    );
    assert_eq!(instant_ms("2026-01-01T00:00:00.5+00:00"), 1_767_225_600_500);
    assert_eq!(instant_ms("not a time"), 0);
    assert_eq!(rfc3339(1_767_225_600_500), "2026-01-01T00:00:00.500+00:00");
    assert_eq!(instant_ms(&rfc3339(1_767_225_600_500)), 1_767_225_600_500);
}

#[test]
fn stamps_never_repeat() {
    let first = next_stamp(None);
    let later = next_stamp(Some(first + 1_000_000));
    assert_eq!(
        later,
        first + 1_000_001,
        "a stamp is strictly after its predecessor"
    );
}

#[tokio::test]
async fn ensure_declares_every_collection_once() {
    let catalog = test_support::catalog();
    catalog.ensure().await.unwrap();
    catalog.ensure().await.unwrap();
    assert_eq!(FlowCatalogDocuments::collections().len(), 5);
}

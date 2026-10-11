//! Guards release visibility and artifact verification sequencing.

const WORKFLOW: &str = include_str!("../../../.github/workflows/release.yml");

#[test]
fn module_release_stays_draft_until_uploaded_artifact_verification_succeeds() {
    let create = WORKFLOW.find("gh release create").unwrap();
    let verify = WORKFLOW[create..].find("- name: Verify").unwrap() + create;
    assert!(
        WORKFLOW[create..verify].contains("--draft"),
        "uploading unverified artifacts must not publish the release"
    );
    let publish = WORKFLOW
        .find("gh release edit")
        .expect("publish explicitly after verification");
    assert!(publish > verify);
    assert!(WORKFLOW[publish..].contains("--draft=false"));
}

#[test]
fn draft_asset_verification_uses_authenticated_download_and_the_actual_loader() {
    let create = WORKFLOW.find("gh release create").unwrap();
    let verify = WORKFLOW[create..].find("- name: Verify").unwrap() + create;
    let publish = WORKFLOW
        .find("gh release edit")
        .expect("explicit post-verification publish");
    let stage = &WORKFLOW[verify..publish];
    assert!(stage.contains("GH_TOKEN:"));
    assert!(stage.contains("gh release download"));
    assert!(stage.contains("sha256sum --check"));
    assert!(stage.contains("TINYFLOWS_MODULE_ARTIFACT="));
    assert!(stage.contains("scripts/verify-module-artifact.sh"));
}

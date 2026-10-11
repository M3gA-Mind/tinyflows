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

#[test]
fn every_native_bundle_loads_its_library_before_packaging() {
    let matrix = WORKFLOW.find("  native-bundles:").unwrap();
    let packaging = WORKFLOW[matrix..]
        .find("- name: Assemble Unix module package")
        .unwrap()
        + matrix;
    let stage = &WORKFLOW[matrix..packaging];
    assert!(stage.contains("- name: Verify Unix native module through TinyBus"));
    assert!(stage.contains("- name: Verify Windows native module through TinyBus"));
    assert_eq!(stage.matches("scripts/verify-module-artifact.sh").count(), 2);
    assert!(stage.contains("$env:TINYFLOWS_MODULE_ARTIFACT"));
    assert!(stage.contains("libtinyflows_module.dylib"));
    assert!(stage.contains("libtinyflows_module.so"));
    assert!(stage.contains("tinyflows_module.dll"));
}

#[test]
fn release_retries_reuse_only_an_existing_draft() {
    let upload = WORKFLOW.find("- name: Create draft release").unwrap();
    let verify = WORKFLOW[upload..].find("- name: Verify").unwrap() + upload;
    let stage = &WORKFLOW[upload..verify];
    assert!(stage.contains("gh release view"));
    assert!(stage.contains("--json isDraft"));
    assert!(stage.contains("[[ \"$is_draft\" == true ]]"));
    assert!(stage.contains("gh release upload"));
    assert!(stage.contains("--clobber"));
}

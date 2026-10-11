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
    assert_eq!(
        stage.matches("scripts/verify-module-artifact.sh").count(),
        2
    );
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

/// Execute the workflow upload block with a local GitHub CLI stand-in. A
/// post-upload failure leaves a draft, and the next attempt must reuse it.
#[cfg(unix)]
#[test]
fn interrupted_release_upload_can_retry_without_republishing() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("release-assets")).unwrap();
    std::fs::write(root.path().join("release-assets/module.tar.gz"), "fixture").unwrap();
    let gh = root.path().join("gh");
    std::fs::write(
        &gh,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$GH_LOG"
case "$2" in
  view) test -f "$GH_DRAFT_STATE"; cat "$GH_DRAFT_STATE" ;;
  create) test ! -f "$GH_DRAFT_STATE"; printf true > "$GH_DRAFT_STATE" ;;
  upload) test "$(cat "$GH_DRAFT_STATE")" = true ;;
  *) exit 2 ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let upload = WORKFLOW.find("- name: Create draft release").unwrap();
    let verify = WORKFLOW[upload..].find("- name: Verify").unwrap() + upload;
    let stage = &WORKFLOW[upload..verify];
    let run = stage.find("        run: |\n").unwrap() + "        run: |\n".len();
    let script: String = stage[run..]
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| format!("{}\n", line.strip_prefix("          ").unwrap_or(line)))
        .collect();
    let paths = std::env::join_paths(
        std::iter::once(root.path().to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();
    let attempt = || {
        std::process::Command::new("bash")
            .args(["-c", &script])
            .current_dir(root.path())
            .env("PATH", &paths)
            .env("RELEASE_TAG", "v-test")
            .env("REPOSITORY", "fixture/repo")
            .env("GH_LOG", root.path().join("calls"))
            .env("GH_DRAFT_STATE", root.path().join("draft"))
            .output()
            .unwrap()
    };
    assert!(attempt().status.success());
    assert!(attempt().status.success());
    let calls = std::fs::read_to_string(root.path().join("calls")).unwrap();
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.starts_with("release create"))
            .count(),
        1
    );
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.starts_with("release upload"))
            .count(),
        1
    );
    std::fs::write(root.path().join("draft"), "false").unwrap();
    assert!(
        !attempt().status.success(),
        "a published release is immutable"
    );
    let after = std::fs::read_to_string(root.path().join("calls")).unwrap();
    assert_eq!(
        after
            .lines()
            .filter(|line| line.starts_with("release upload"))
            .count(),
        1
    );
}

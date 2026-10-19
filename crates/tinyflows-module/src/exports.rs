//! TinyBus ABI exports with explicit member lists for each provided interface.
//! The convenience export macro accepts one member list; this module provides
//! two objects, so it declares the manifest through the standard ABI types.
use std::sync::OnceLock;
use tinybus::module::{abi::*, manifest::*};
use tinybus::{BusName, InterfaceName, InterfaceVersion, MemberName, ObjectPath, Version};

#[cfg_attr(not(feature = "static-link"), unsafe(no_mangle))]
pub static TINYBUS_MODULE_ABI_V1: TbAbiDescriptor =
    TbAbiDescriptor::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

#[cfg_attr(not(feature = "static-link"), unsafe(no_mangle))]
pub extern "C" fn tinybus_module_manifest_v1() -> TbSlice {
    std::panic::catch_unwind(manifest).unwrap_or(TbSlice {
        ptr: std::ptr::null(),
        len: 0,
    })
}
fn manifest() -> TbSlice {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = BYTES.get_or_init(|| {
        let version = Version::parse(env!("CARGO_PKG_VERSION")).expect("package semver");
        let provided = |interface: &str, methods: &[&str]| ProvidedInterface {
            version: InterfaceVersion::provided(
                InterfaceName::new(interface).expect("static interface"),
                version.clone(),
            ),
            methods: methods
                .iter()
                .map(|m| MemberName::new(*m).expect("static member"))
                .collect(),
            signals: vec![],
        };
        serde_json::to_vec(&ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: env!("CARGO_PKG_NAME").into(),
                version: version.clone(),
                description: "Workflow authoring and schedule runtime".into(),
                homepage: None,
                license: env!("CARGO_PKG_LICENSE").into(),
            },
            bus_name: BusName::new(tinyflows_bus::BUS_NAME).expect("static bus name"),
            object_path: ObjectPath::new(tinyflows_bus::OBJECT_PATH).expect("static object path"),
            provides: vec![
                provided(tinyflows_bus::INTERFACE, tinyflows_bus::METHODS),
                provided(
                    tinyflows_bus::copilot::INTERFACE,
                    tinyflows_bus::copilot::METHODS,
                ),
                provided(tinyflows_bus::runs::INTERFACE, tinyflows_bus::runs::METHODS),
                provided(
                    tinyflows_bus::catalog::INTERFACE,
                    tinyflows_bus::catalog::METHODS,
                ),
                provided(
                    tinyflows_bus::schedule::INTERFACE,
                    tinyflows_bus::schedule::METHODS,
                ),
                provided(
                    tinyflows_bus::portability::INTERFACE,
                    tinyflows_bus::portability::METHODS,
                ),
            ],
            requires: vec![
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(tinyflows_bus::copilot::HOST_INTERFACE)
                            .expect("static interface"),
                        version.clone(),
                    ),
                    optional: true,
                    reason: "Copilot tools call the host callback bound to the invocation scope"
                        .into(),
                },
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(tinyflows_bus::HOST_INTERFACE)
                            .expect("static interface"),
                        version.clone(),
                    ),
                    optional: true,
                    reason: String::new(),
                },
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(tinyflows_bus::runs::CAPS_INTERFACE)
                            .expect("static interface"),
                        Version::new(
                            u64::from(tinyflows_bus::runs::CAPS_CONTRACT_VERSION),
                            0,
                            0,
                        ),
                    ),
                    optional: true,
                    reason: "Runs call the exact host callback address bound to their owner scope"
                        .into(),
                },
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(tinyflows_bus::catalog::STORAGE_INTERFACE)
                            .expect("static interface"),
                        version.clone(),
                    ),
                    optional: true,
                    reason:
                        "Catalog requests require a host callback bound to the acting agent scope"
                            .into(),
                },
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(tinyflows_bus::catalog::DRAFT_PROMOTION_HOST_INTERFACE)
                            .expect("static interface"),
                        version.clone(),
                    ),
                    optional: true,
                    reason: "Draft promotion invokes the scope-bound host save path".into(),
                },
                Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(
                            tinyflows_bus::catalog::APPROVAL_MANIFEST_HOST_INTERFACE,
                        )
                        .expect("static interface"),
                        version.clone(),
                    ),
                    optional: true,
                    reason: "Approval manifests use the scope-bound host policy classifier".into(),
                },
            ],
            environment: vec![],
            capabilities: vec![],
            lazy_init: false,
            worker_threads: 2,
            on_panic: PanicPolicy::Detach,
        })
        .expect("manifest serializes")
    });
    TbSlice {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    }
}

/// Start the standard TinyBus module runtime.
///
/// # Safety
/// `host` and `out` must satisfy the TinyBus ABI v1 pointer and lifetime contract.
#[cfg_attr(not(feature = "static-link"), unsafe(no_mangle))]
pub unsafe extern "C" fn tinybus_module_init_v1(
    host: *const TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    unsafe { tinybus_module::start_module(host, out, 2, true, super::service::setup) }
}

/// Entry points and parsed manifest for a host that links this module instead
/// of loading the `cdylib` (local development and host unit tests).
///
/// # Errors
/// Returns an error if the generated manifest is invalid.
#[cfg(feature = "static-link")]
pub fn linked_module() -> tinybus::Result<tinybus::module::LinkedModule> {
    unsafe {
        tinybus::module::LinkedModule::from_exports(
            &TINYBUS_MODULE_ABI_V1,
            tinybus_module_manifest_v1,
            tinybus_module_init_v1,
        )
    }
}

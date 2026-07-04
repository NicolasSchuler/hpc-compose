use std::collections::BTreeMap;
use std::path::Path;

use proptest::prelude::*;

use super::*;
use crate::spec::{
    ComposeSpec, DependsOnConditionSpec, DependsOnSpec, EnvironmentSpec, HostMpiConfig, MpiConfig,
    MpiLauncher, MpiType, ReadinessSpec, RuntimeConfig, ServiceDependency, ServiceEnrootConfig,
    ServiceFailureMode, ServiceFailurePolicy, ServiceFailurePolicySpec, ServiceHookContext,
    ServiceHookSpec, ServicePlacementSpec, ServiceRuntimeConfig, ServiceSlurmConfig, ServiceSpec,
};

fn service(image: &str) -> ServiceSpec {
    ServiceSpec {
        image: Some(image.to_string()),
        command: None,
        entrypoint: None,
        script: None,
        env_file: None,
        environment: EnvironmentSpec::None,
        volumes: Vec::new(),
        working_dir: None,
        depends_on: DependsOnSpec::None,
        readiness: None,
        healthcheck: None,
        assertions: None,
        software_env: crate::spec::SoftwareEnvConfig::default(),
        slurm: ServiceSlurmConfig::default(),
        runtime: ServiceRuntimeConfig::default(),
        enroot: ServiceEnrootConfig::default(),
    }
}

#[test]
fn resource_profile_defaults_preserve_explicit_slurm_values() {
    let mut services = BTreeMap::new();
    services.insert("app".to_string(), service("alpine:latest"));

    let mut profiles = BTreeMap::new();
    profiles.insert(
        "gpu-small".to_string(),
        ResourceProfile {
            partition: Some("gpu".to_string()),
            mem: Some("16G".to_string()),
            gpus: Some(1),
            cpus_per_task: Some(4),
            ..ResourceProfile::default()
        },
    );

    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        name: Some("profile-demo".to_string()),
        runtime: RuntimeConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        slurm: SlurmConfig {
            resources: Some("gpu-small".to_string()),
            mem: Some("32G".to_string()),
            ..SlurmConfig::default()
        },
        services,
        sweep: None,
    };

    let plan = build_plan_with_options(
        Path::new("."),
        spec,
        PlanOptions {
            resource_profiles: profiles,
            ..PlanOptions::default()
        },
    )
    .expect("plan");

    assert_eq!(plan.slurm.partition.as_deref(), Some("gpu"));
    assert_eq!(plan.slurm.gpus, Some(1));
    assert_eq!(plan.slurm.cpus_per_task, Some(4));
    assert_eq!(plan.slurm.mem.as_deref(), Some("32G"));
}

#[test]
fn resource_profile_defaults_include_new_resource_fields() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let mut profiles = BTreeMap::new();
    profiles.insert(
        "accelerated".to_string(),
        ResourceProfile {
            gres: Some("gpu:a100:2".into()),
            gpus_per_node: Some(2),
            gpus_per_task: Some(1),
            cpus_per_gpu: Some(8),
            mem_per_gpu: Some("24G".into()),
            gpu_bind: Some("closest".into()),
            cpu_bind: Some("cores".into()),
            mem_bind: Some("local".into()),
            distribution: Some("block:block".into()),
            hint: Some("nomultithread".into()),
            constraint: Some("a100".into()),
            ..ResourceProfile::default()
        },
    );

    let plan = build_plan_with_options(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                resources: Some("accelerated".into()),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([("app".into(), service("redis:7"))]),
        },
        PlanOptions {
            resource_profiles: profiles,
            ..PlanOptions::default()
        },
    )
    .expect("profile defaults");

    assert_eq!(plan.slurm.gres.as_deref(), Some("gpu:a100:2"));
    assert_eq!(plan.slurm.gpus_per_node, Some(2));
    assert_eq!(plan.slurm.gpus_per_task, Some(1));
    assert_eq!(plan.slurm.cpus_per_gpu, Some(8));
    assert_eq!(plan.slurm.mem_per_gpu.as_deref(), Some("24G"));
    assert_eq!(plan.slurm.gpu_bind.as_deref(), Some("closest"));
    assert_eq!(plan.slurm.cpu_bind.as_deref(), Some("cores"));
    assert_eq!(plan.slurm.mem_bind.as_deref(), Some("local"));
    assert_eq!(plan.slurm.distribution.as_deref(), Some("block:block"));
    assert_eq!(plan.slurm.hint.as_deref(), Some("nomultithread"));
    assert_eq!(plan.slurm.constraint.as_deref(), Some("a100"));
}

proptest! {
    #[test]
    fn property_node_ranges_resolve_to_sorted_in_range_sets(
        allocation_nodes in 1u32..32,
        raw_start in 0u32..64,
        raw_width in 0u32..64,
    ) {
        let start = raw_start % allocation_nodes;
        let width = raw_width % (allocation_nodes - start);
        let end = start + width;
        let expr = if start == end {
            start.to_string()
        } else {
            format!("{start}-{end}")
        };
        let indices = parse_node_index_expr(&expr, allocation_nodes, "placement").expect("indices");
        prop_assert!(!indices.is_empty());
        prop_assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
        prop_assert!(indices.iter().all(|index| *index < allocation_nodes));
        prop_assert_eq!(indices.first().copied(), Some(start));
        prop_assert_eq!(indices.last().copied(), Some(end));
    }

    #[test]
    fn property_topological_order_places_dependencies_first(service_count in 1usize..8) {
        let mut services = BTreeMap::new();
        for index in 0..service_count {
            let name = format!("s{index}");
            let mut spec = service("redis:7");
            if index > 0 {
                spec.depends_on = DependsOnSpec::List(vec![format!("s{}", index - 1)]);
            }
            services.insert(name, spec);
        }
        let spec = ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services,
            sweep: None,
        };
        let plan = build_plan(Path::new("."), spec).expect("plan");
        let positions = plan
            .ordered_services
            .iter()
            .enumerate()
            .map(|(index, service)| (service.name.clone(), index))
            .collect::<BTreeMap<_, _>>();
        for index in 1..service_count {
            let dependent = positions[&format!("s{index}")];
            let dependency = positions[&format!("s{}", index - 1)];
            prop_assert!(dependency < dependent);
        }
    }
}

#[test]
fn bare_images_normalize_to_docker_uri() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([("redis".into(), service("redis:7"))]),
    };
    let plan = build_plan(Path::new("."), spec).expect("plan");
    assert_eq!(
        plan.ordered_services[0].image,
        ImageSource::Remote("docker://redis:7".into())
    );
}

#[test]
fn host_backend_allows_command_without_image() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                image: None,
                command: Some(CommandSpec::String("module list".into())),
                ..service("ignored:latest")
            },
        )]),
    };

    let plan = build_plan(Path::new("."), spec).expect("host plan");
    assert_eq!(plan.ordered_services[0].image, ImageSource::Host);
}

#[test]
fn host_backend_rejects_service_volumes() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                image: None,
                command: Some(CommandSpec::String("/bin/true".into())),
                volumes: vec!["./app:/workspace".into()],
                ..service("ignored:latest")
            },
        )]),
    };

    let err = build_plan(Path::new("."), spec).expect_err("host volumes");
    assert!(err.to_string().contains("volumes"));
    assert!(err.to_string().contains("runtime.backend=host"));
}

#[test]
fn host_backend_rejects_host_mpi_bind_paths() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                image: None,
                command: Some(CommandSpec::String("/bin/true".into())),
                slurm: ServiceSlurmConfig {
                    mpi: Some(MpiConfig {
                        mpi_type: MpiType::new("pmix").expect("mpi type"),
                        profile: None,
                        implementation: None,
                        launcher: MpiLauncher::default(),
                        expected_ranks: None,
                        host_mpi: Some(HostMpiConfig {
                            bind_paths: vec!["/opt/mpi:/opt/mpi:ro".into()],
                            env: EnvironmentSpec::None,
                        }),
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("ignored:latest")
            },
        )]),
    };

    let err = build_plan(Path::new("."), spec).expect_err("host mpi binds");
    assert!(err.to_string().contains("host_mpi.bind_paths"));
    assert!(err.to_string().contains("runtime.backend=host"));
}

#[test]
fn non_pyxis_backends_accept_sif_and_reject_sqsh() {
    let project = Path::new("/tmp/project");
    let source = normalize_image(
        Some("./image.sif"),
        RuntimeBackend::Apptainer,
        project,
        "app",
    )
    .expect("sif image");
    assert!(matches!(source, ImageSource::LocalSif(path) if path.ends_with("image.sif")));

    let err = normalize_image(
        Some("./image.sqsh"),
        RuntimeBackend::Apptainer,
        project,
        "app",
    )
    .expect_err("sqsh rejected");
    assert!(
        err.to_string()
            .contains("expects a remote image or local .sif")
    );
}

#[test]
fn read_only_volume_mode_is_preserved() {
    let mount = normalize_mount("./data:/data:ro", Path::new("/tmp/project")).expect("mount");
    assert_eq!(mount, "/tmp/project/data:/data:ro");
}

#[test]
fn build_execution_rejects_ambiguous_mixed_forms() {
    let result = build_execution(
        Some(&CommandSpec::Vec(vec!["/bin/app".into()])),
        Some(&CommandSpec::String("serve".into())),
        None,
        "app",
    );
    assert!(result.is_err());
}

#[test]
fn build_execution_allows_exec_form() {
    let execution = build_execution(
        Some(&CommandSpec::Vec(vec!["/bin/app".into()])),
        Some(&CommandSpec::Vec(vec![
            "serve".into(),
            "--port".into(),
            "8080".into(),
        ])),
        None,
        "app",
    )
    .expect("exec");

    assert_eq!(
        execution,
        ExecutionSpec::Exec(vec![
            "/bin/app".into(),
            "serve".into(),
            "--port".into(),
            "8080".into()
        ])
    );
}

#[test]
fn working_dir_requires_explicit_command() {
    let result = build_execution(None, None, Some("/work"), "app");
    assert!(result.is_err());
}

#[test]
fn container_hooks_require_explicit_command_or_entrypoint() {
    let mut app = service("redis:7");
    app.slurm.prologue = Some(ServiceHookSpec {
        context: ServiceHookContext::Container,
        script: "echo prepare".into(),
    });
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([("app".into(), app)]),
    };

    let err = build_plan(Path::new("."), spec).expect_err("image default cannot be wrapped");
    assert!(err.to_string().contains("container-context"));
    assert!(err.to_string().contains("command or entrypoint"));
}

#[test]
fn prepare_mounts_force_rebuild() {
    let spec = PrepareSpec {
        commands: vec!["echo hello".into()],
        mounts: vec!["./data:/data".into()],
        env: EnvironmentSpec::None,
        root: true,
    };
    let prepare = build_prepare_plan(spec, Path::new("/tmp/project"), "svc", "x-runtime.prepare")
        .expect("prepare");
    assert!(prepare.force_rebuild);
    assert_eq!(prepare.mounts, vec!["/tmp/project/data:/data"]);
}

#[test]
fn topo_sort_orders_dependencies() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "app".into(),
                ServiceSpec {
                    depends_on: DependsOnSpec::List(vec!["redis".into()]),
                    ..service("redis:7")
                },
            ),
            ("redis".into(), service("redis:7")),
        ]),
    };

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let plan = build_plan(&compose, spec).expect("plan");
    let names = plan
        .ordered_services
        .iter()
        .map(|svc| svc.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["redis", "app"]);
}

#[test]
fn build_plan_rejects_reserved_runtime_mount_destination() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                volumes: vec!["./data:/hpc-compose/job".into()],
                ..service("redis:7")
            },
        )]),
    };

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let err = build_plan(&compose, spec).expect_err("reserved mount");
    assert!(
        err.to_string()
            .contains("reserved runtime mount destination")
    );
    assert!(err.to_string().contains("/hpc-compose/job"));
}

#[test]
fn cache_dir_policy_flags_tmp() {
    let issue = cache_path_policy_issue(Path::new("/tmp/hpc-compose")).expect("issue");
    assert!(issue.contains("not shared"));
}

#[test]
fn runtime_root_policy_flags_node_local_override() {
    let issue = runtime_root_policy_issue(Path::new("/tmp/runs")).expect("issue");
    assert!(issue.contains("not shared"));
    assert!(issue.contains("x-slurm.runtime_root"));
    assert!(runtime_root_policy_issue(Path::new("/shared/runs")).is_none());
}

#[test]
fn registry_host_defaults_to_docker_hub_for_bare_refs() {
    assert_eq!(
        registry_host_for_remote("docker://redis:7"),
        "registry-1.docker.io"
    );
    assert_eq!(
        registry_host_for_remote("docker://python:3.11-slim"),
        "registry-1.docker.io"
    );
    assert_eq!(
        registry_host_for_remote("docker://library/redis:7"),
        "registry-1.docker.io"
    );
}

#[test]
fn registry_host_extracts_explicit_registry_hosts() {
    assert_eq!(
        registry_host_for_remote("docker://ghcr.io/ggerganov/llama.cpp:server-cuda"),
        "ghcr.io"
    );
    assert_eq!(
        registry_host_for_remote("docker://registry.scc.kit.edu#proj/app:latest"),
        "registry.scc.kit.edu"
    );
    assert_eq!(
        registry_host_for_remote("docker://localhost:5000/app:latest"),
        "localhost:5000"
    );
}

#[test]
fn readiness_is_cloned_into_plan() {
    let mut svc = service("redis:7");
    svc.readiness = Some(ReadinessSpec::Sleep { seconds: 5 });
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([("redis".into(), svc)]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let plan = build_plan(&compose, spec).expect("plan");
    assert_eq!(
        plan.ordered_services[0].readiness,
        Some(ReadinessSpec::Sleep { seconds: 5 })
    );
}

#[test]
fn build_plan_rejects_empty_services_and_accepts_multi_node_single_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: None,
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::new(),
        },
    )
    .expect_err("empty services");
    assert!(err.to_string().contains("at least one service"));

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: None,
            slurm: SlurmConfig {
                nodes: Some(2),
                ntasks_per_node: Some(4),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([("app".into(), service("redis:7"))]),
        },
    )
    .expect("multi node");
    assert_eq!(plan.slurm.allocation_nodes(), 2);
    assert_eq!(plan.ordered_services.len(), 1);
    assert_eq!(
        plan.ordered_services[0].placement.mode,
        ServicePlacementMode::Distributed
    );
    assert_eq!(plan.ordered_services[0].placement.nodes, 2);
    assert_eq!(plan.ordered_services[0].placement.ntasks_per_node, Some(4));
}

#[test]
fn build_plan_rejects_overlapping_full_allocation_services() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        nodes: Some(2),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "b".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        nodes: Some(2),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };

    let err = build_plan(&compose, spec).expect_err("overlapping distributed services");
    assert!(err.to_string().contains("overlap"));
}

#[test]
fn build_plan_accepts_disjoint_partitioned_services() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([
                (
                    "a".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-3".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "b".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("4-7".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        },
    )
    .expect("partitioned plan");

    let a = plan
        .ordered_services
        .iter()
        .find(|service| service.name == "a")
        .expect("a");
    let b = plan
        .ordered_services
        .iter()
        .find(|service| service.name == "b")
        .expect("b");
    assert_eq!(a.placement.mode, ServicePlacementMode::Partitioned);
    assert_eq!(a.placement.nodes, 4);
    assert_eq!(a.placement.node_indices, Some(vec![0, 1, 2, 3]));
    assert_eq!(b.placement.node_indices, Some(vec![4, 5, 6, 7]));
}

#[test]
fn plan_resolves_sparse_placement_with_start_index_and_exclude() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "workers".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_count: Some(3),
                            start_index: Some(2),
                            exclude: Some("3,5".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            )]),
        },
    )
    .expect("sparse placement");

    let workers = &plan.ordered_services[0];
    assert_eq!(workers.placement.mode, ServicePlacementMode::Partitioned);
    assert_eq!(workers.placement.nodes, 3);
    assert_eq!(workers.placement.node_indices, Some(vec![2, 4, 6]));
    assert_eq!(workers.placement.exclude_indices, vec![3, 5]);
}

#[test]
fn build_plan_resolves_node_percent_with_start_index_and_exclude() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(10),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "workers".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_percent: Some(50),
                            start_index: Some(2),
                            exclude: Some("3,7".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            )]),
        },
    )
    .expect("percent placement");

    let workers = &plan.ordered_services[0];
    assert_eq!(workers.placement.nodes, 4);
    assert_eq!(workers.placement.node_indices, Some(vec![2, 4, 5, 6]));
    assert_eq!(workers.placement.exclude_indices, vec![3, 7]);
}

#[test]
fn build_plan_rejects_start_index_outside_allocation() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_count: Some(1),
                            start_index: Some(2),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        },
    )
    .expect_err("start index out of allocation");
    assert!(
        err.to_string()
            .contains("start_index=2 is outside the 2 node allocation")
    );
}

#[test]
fn build_plan_rejects_node_count_when_start_index_leaves_too_few_nodes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(3),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_count: Some(2),
                            start_index: Some(2),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        },
    )
    .expect_err("not enough eligible nodes");
    let text = err.to_string();
    assert!(text.contains("requests 2 node(s)"), "{text}");
    assert!(text.contains("only 1 eligible node(s)"), "{text}");
}

#[test]
fn build_plan_resolves_explicit_full_allocation_range_as_distributed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(4),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "trainer".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-3".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            )]),
        },
    )
    .expect("full allocation placement");

    let trainer = &plan.ordered_services[0];
    assert_eq!(trainer.placement.mode, ServicePlacementMode::Distributed);
    assert_eq!(trainer.placement.nodes, 4);
    assert_eq!(trainer.placement.node_indices, None);
}

#[test]
fn build_plan_rejects_service_nodes_mismatch_with_explicit_placement() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(4),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        nodes: Some(2),
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        },
    )
    .expect_err("nodes mismatch");
    assert!(
        err.to_string()
            .contains("sets x-slurm.nodes=2 but x-slurm.placement resolves to 1 node"),
        "{err:#}"
    );
}

#[test]
fn build_plan_rejects_unknown_share_with_target_with_context() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "sidecar".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            share_with: Some("missing".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        },
    )
    .expect_err("unknown share_with target");
    let text = format!("{err:#}");
    assert!(
        text.contains("service 'sidecar' x-slurm.placement.share_with references 'missing'"),
        "{text}"
    );
    assert!(text.contains("service 'missing' does not exist"), "{text}");
}

#[test]
fn build_plan_share_with_recomputes_task_geometry_for_sharer() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(4),
                ntasks: Some(8),
                ntasks_per_node: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([
                (
                    "worker".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-1".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
                (
                    "sidecar".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            ntasks: Some(1),
                            placement: Some(ServicePlacementSpec {
                                share_with: Some("worker".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
            ]),
        },
    )
    .expect("shared placement");

    let by_name = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.as_str(), &service.placement))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_name["worker"].node_indices, Some(vec![0, 1]));
    assert_eq!(by_name["sidecar"].node_indices, Some(vec![0, 1]));
    assert_eq!(by_name["sidecar"].ntasks, Some(1));
    assert_eq!(by_name["sidecar"].ntasks_per_node, None);
    assert!(by_name["sidecar"].allow_overlap);
}

#[test]
fn plan_step_task_geometry_respects_service_overrides_before_allocation_defaults() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(6),
                ntasks: Some(24),
                ntasks_per_node: Some(4),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([
                (
                    "inherits".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-1".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("alpine:3.20")
                    },
                ),
                (
                    "task_override".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            ntasks: Some(6),
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("2-3".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
                (
                    "per_node_override".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            ntasks_per_node: Some(2),
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("4-5".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("ubuntu:24.04")
                    },
                ),
            ]),
        },
    )
    .expect("task geometry plan");

    let by_name = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.as_str(), &service.placement))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_name["inherits"].ntasks, Some(24));
    assert_eq!(by_name["inherits"].ntasks_per_node, Some(4));
    assert_eq!(by_name["task_override"].ntasks, Some(6));
    assert_eq!(by_name["task_override"].ntasks_per_node, None);
    assert_eq!(by_name["per_node_override"].ntasks, Some(24));
    assert_eq!(by_name["per_node_override"].ntasks_per_node, Some(2));
}

#[test]
fn plan_rejects_missing_resource_profile_reference() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let err = build_plan_with_options(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                resources: Some("missing".into()),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([("app".into(), service("redis:7"))]),
        },
        PlanOptions::default(),
    )
    .expect_err("missing profile");

    assert!(
        err.to_string()
            .contains("references undefined resource profile 'missing'")
    );
}

#[test]
fn plan_resolves_cache_dir_with_missing_synthetic_spec_path_and_project_override() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let project_dir = tmpdir.path().join("project");

    let explicit = build_plan_with_options(
        Path::new("synthetic/compose.yaml"),
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                cache_dir: Some("./cache".into()),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([("app".into(), service("redis:7"))]),
        },
        PlanOptions {
            project_dir_override: Some(project_dir.clone()),
            allow_missing_spec_path: true,
            ..PlanOptions::default()
        },
    )
    .expect("synthetic explicit cache plan");
    assert_eq!(explicit.project_dir, project_dir);
    assert_eq!(explicit.cache_dir, explicit.project_dir.join("cache"));

    let fallback = build_plan_with_options(
        Path::new("synthetic/compose.yaml"),
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([("app".into(), service("redis:7"))]),
        },
        PlanOptions {
            cache_dir_default: Some(Path::new("/shared/cache").into()),
            project_dir_override: Some(project_dir),
            allow_missing_spec_path: true,
            ..PlanOptions::default()
        },
    )
    .expect("synthetic default cache plan");
    assert_eq!(fallback.cache_dir, Path::new("/shared/cache"));
}

#[test]
fn plan_rejects_service_nodes_mismatch_and_mpi_expected_rank_mismatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let nodes_mismatch = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(4),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    nodes: Some(2),
                    placement: Some(ServicePlacementSpec {
                        node_range: Some("0".into()),
                        ..ServicePlacementSpec::default()
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, nodes_mismatch).expect_err("node mismatch");
    assert!(err.to_string().contains("resolves to 1 node"));

    let rank_mismatch = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "trainer".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    ntasks_per_node: Some(2),
                    mpi: Some(MpiConfig {
                        mpi_type: MpiType::new("pmix").expect("mpi type"),
                        profile: None,
                        implementation: None,
                        launcher: MpiLauncher::default(),
                        expected_ranks: Some(5),
                        host_mpi: None,
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("python:3.11-slim")
            },
        )]),
    };
    let err = build_plan(&compose, rank_mismatch).expect_err("rank mismatch");
    assert!(err.to_string().contains("expected_ranks=5"));
    assert!(err.to_string().contains("launches 4 rank"));
}

#[test]
fn plan_normalizes_host_mpi_bind_paths_env_and_rejects_backend_prepare_conflicts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "trainer".into(),
                ServiceSpec {
                    environment: EnvironmentSpec::Map(BTreeMap::from([(
                        "APP_ENV".into(),
                        "prod".into(),
                    )])),
                    slurm: ServiceSlurmConfig {
                        mpi: Some(MpiConfig {
                            mpi_type: MpiType::new("pmix").expect("mpi type"),
                            profile: None,
                            implementation: None,
                            launcher: MpiLauncher::default(),
                            expected_ranks: None,
                            host_mpi: Some(HostMpiConfig {
                                bind_paths: vec!["./mpi:/opt/mpi:ro".into()],
                                env: EnvironmentSpec::Map(BTreeMap::from([(
                                    "MPI_HOME".into(),
                                    "/opt/mpi".into(),
                                )])),
                            }),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            )]),
        },
    )
    .expect("host mpi plan");
    let planned = &plan.ordered_services[0];
    assert_eq!(
        planned.environment,
        vec![
            ("APP_ENV".to_string(), "prod".to_string()),
            ("MPI_HOME".to_string(), "/opt/mpi".to_string()),
        ]
    );
    assert_eq!(
        planned.volumes,
        vec![format!(
            "{}:/opt/mpi:ro",
            plan.project_dir.join("mpi").display()
        )]
    );

    let host_prepare = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                image: None,
                command: Some(CommandSpec::String("/bin/true".into())),
                runtime: ServiceRuntimeConfig {
                    prepare: Some(PrepareSpec {
                        commands: vec!["echo prepare".into()],
                        mounts: Vec::new(),
                        env: EnvironmentSpec::None,
                        root: true,
                    }),
                },
                ..service("ignored:latest")
            },
        )]),
    };
    let err = build_plan(&compose, host_prepare).expect_err("host prepare conflict");
    assert!(err.to_string().contains("image prepare"));
    assert!(err.to_string().contains("runtime.backend=host"));

    let enroot_prepare_with_sif = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                enroot: ServiceEnrootConfig {
                    prepare: Some(PrepareSpec {
                        commands: vec!["echo prepare".into()],
                        mounts: Vec::new(),
                        env: EnvironmentSpec::None,
                        root: true,
                    }),
                },
                ..service("docker://redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, enroot_prepare_with_sif).expect_err("enroot prepare conflict");
    assert!(err.to_string().contains("x-enroot.prepare"));
    assert!(err.to_string().contains("runtime.backend=apptainer"));
}

#[test]
fn build_plan_resolves_percent_with_ceil_minimum_one() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let plan = build_plan(
        &compose,
        ComposeSpec {
            secrets: BTreeMap::new(),
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            sweep: None,
            services: BTreeMap::from([(
                "workers".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_percent: Some(60),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            )]),
        },
    )
    .expect("percent plan");

    let workers = &plan.ordered_services[0];
    assert_eq!(workers.placement.nodes, 5);
    assert_eq!(workers.placement.node_indices, Some(vec![0, 1, 2, 3, 4]));
}

#[test]
fn build_plan_rejects_accidental_overlap_unless_allowed_or_shared() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let overlapping = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(8),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-3".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "b".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("3-5".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };
    let err = build_plan(&compose, overlapping).expect_err("overlap");
    assert!(err.to_string().contains("overlap"));

    let allowed = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(8),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-3".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "b".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("3-5".into()),
                            allow_overlap: true,
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };
    build_plan(&compose, allowed).expect("overlap allowed");

    let shared = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(8),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "ps".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            share_with: Some("workers".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "workers".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("2-5".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };
    let plan = build_plan(&compose, shared).expect("share placement");
    let ps = plan
        .ordered_services
        .iter()
        .find(|service| service.name == "ps")
        .expect("ps");
    let workers = plan
        .ordered_services
        .iter()
        .find(|service| service.name == "workers")
        .expect("workers");
    assert_eq!(ps.placement.node_indices, workers.placement.node_indices);
}

#[test]
fn build_plan_rejects_invalid_partitioned_placements() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let out_of_bounds = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "a".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    placement: Some(ServicePlacementSpec {
                        node_range: Some("0-2".into()),
                        ..ServicePlacementSpec::default()
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, out_of_bounds).expect_err("out of bounds");
    assert!(err.to_string().contains("only has 2 node"));

    let empty_after_exclude = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "a".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    placement: Some(ServicePlacementSpec {
                        node_range: Some("0-1".into()),
                        exclude: Some("0-1".into()),
                        ..ServicePlacementSpec::default()
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, empty_after_exclude).expect_err("empty placement");
    assert!(err.to_string().contains("empty node set"));

    let cycle = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            share_with: Some("b".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "b".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            share_with: Some("a".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };
    let err = build_plan(&compose, cycle).expect_err("share cycle");
    let err_text = format!("{err:#}");
    assert!(err_text.contains("cycle"), "{err_text}");
}

#[test]
fn build_plan_rejects_distributed_readiness_with_localhost_semantics() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "trainer".into(),
            ServiceSpec {
                readiness: Some(ReadinessSpec::Tcp {
                    host: None,
                    port: 29500,
                    timeout_seconds: None,
                }),
                ..service("python:3.11-slim")
            },
        )]),
    };

    let err = build_plan(&compose, spec).expect_err("distributed localhost readiness");
    assert!(err.to_string().contains("localhost semantics"));
}

#[test]
fn build_plan_rejects_non_primary_placement_readiness_with_localhost_semantics() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let explicit_non_primary = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                readiness: Some(ReadinessSpec::Tcp {
                    host: None,
                    port: 6379,
                    timeout_seconds: None,
                }),
                slurm: ServiceSlurmConfig {
                    placement: Some(ServicePlacementSpec {
                        node_range: Some("1".into()),
                        ..ServicePlacementSpec::default()
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err =
        build_plan(&compose, explicit_non_primary).expect_err("non-primary localhost readiness");
    assert!(err.to_string().contains("localhost semantics"));

    let shared_multi_node = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig {
            nodes: Some(4),
            ..SlurmConfig::default()
        },
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "ps".into(),
                ServiceSpec {
                    readiness: Some(ReadinessSpec::Tcp {
                        host: None,
                        port: 6379,
                        timeout_seconds: None,
                    }),
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            share_with: Some("workers".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "workers".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-1".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("python:3.11-slim")
                },
            ),
        ]),
    };
    let err = build_plan(&compose, shared_multi_node).expect_err("shared localhost readiness");
    assert!(err.to_string().contains("localhost semantics"));
}

#[test]
fn build_prepare_and_execution_cover_error_and_string_variants() {
    let err = build_prepare_plan(
        PrepareSpec {
            commands: Vec::new(),
            mounts: Vec::new(),
            env: EnvironmentSpec::None,
            root: true,
        },
        Path::new("/tmp/project"),
        "svc",
        "x-runtime.prepare",
    )
    .expect_err("missing commands");
    assert!(err.to_string().contains("prepare.commands"));

    let prepared = build_prepare_plan(
        PrepareSpec {
            commands: vec!["echo hi".into()],
            mounts: Vec::new(),
            env: EnvironmentSpec::Map(BTreeMap::from([("A".into(), "B".into())])),
            root: false,
        },
        Path::new("/tmp/project"),
        "svc",
        "x-runtime.prepare",
    )
    .expect("prepared");
    assert!(!prepared.force_rebuild);
    assert_eq!(prepared.env, vec![("A".into(), "B".into())]);
    assert!(!prepared.root);

    assert_eq!(
        build_execution(
            None,
            Some(&CommandSpec::String("echo hi".into())),
            None,
            "svc"
        )
        .expect("shell"),
        ExecutionSpec::Shell("echo hi".into())
    );
    assert_eq!(
        build_execution(
            Some(&CommandSpec::String("python".into())),
            None,
            None,
            "svc"
        )
        .expect("entry shell"),
        ExecutionSpec::Shell("python".into())
    );
    assert_eq!(
        build_execution(
            Some(&CommandSpec::String("python".into())),
            Some(&CommandSpec::String("-m main".into())),
            None,
            "svc"
        )
        .expect("combined"),
        ExecutionSpec::Shell("python -m main".into())
    );
}

#[test]
fn topo_sort_and_normalize_helpers_cover_error_branches() {
    let services = BTreeMap::from([(
        "app".into(),
        PlannedService {
            name: "app".into(),
            image: ImageSource::Remote("docker://redis:7".into()),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: vec![ServiceDependency {
                name: "missing".into(),
                condition: DependencyCondition::ServiceStarted,
                implicit: false,
            }],
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
        },
    )]);
    let err = topo_sort(&services).expect_err("missing dep");
    assert!(err.to_string().contains("undefined service"));

    let cycle = BTreeMap::from([
        (
            "a".into(),
            PlannedService {
                name: "a".into(),
                image: ImageSource::Remote("docker://redis:7".into()),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: vec![ServiceDependency {
                    name: "b".into(),
                    condition: DependencyCondition::ServiceStarted,
                    implicit: false,
                }],
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
            },
        ),
        (
            "b".into(),
            PlannedService {
                name: "b".into(),
                image: ImageSource::Remote("docker://redis:7".into()),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: vec![ServiceDependency {
                    name: "a".into(),
                    condition: DependencyCondition::ServiceStarted,
                    implicit: false,
                }],
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
            },
        ),
    ]);
    let err = topo_sort(&cycle).expect_err("cycle");
    assert!(err.to_string().contains("dependency cycle"));

    let err = normalize_mount("/host-only", Path::new("/tmp/project")).expect_err("mount");
    assert!(err.to_string().contains("host_path:container_path"));
}

#[test]
fn image_and_path_normalizers_cover_remaining_variants() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_sqsh = tmpdir.path().join("image.sqsh");
    std::fs::write(&local_sqsh, "x").expect("sqsh");

    assert_eq!(
        normalize_image(
            Some(local_sqsh.to_str().expect("path")),
            RuntimeBackend::Pyxis,
            tmpdir.path(),
            "svc"
        )
        .expect("local"),
        ImageSource::LocalSqsh(local_sqsh.clone())
    );
    assert_eq!(
        normalize_image(
            Some("docker://redis:7"),
            RuntimeBackend::Pyxis,
            tmpdir.path(),
            "svc"
        )
        .expect("remote"),
        ImageSource::Remote("docker://redis:7".into())
    );
    assert_eq!(
        normalize_image(
            Some("docker://registry.example/app.sif"),
            RuntimeBackend::Pyxis,
            tmpdir.path(),
            "svc"
        )
        .expect("remote sif-like uri"),
        ImageSource::Remote("docker://registry.example/app.sif".into())
    );

    let err = normalize_image(
        Some("oci://redis:7"),
        RuntimeBackend::Pyxis,
        tmpdir.path(),
        "svc",
    )
    .expect_err("scheme");
    assert!(err.to_string().contains("unsupported image scheme"));
    let err = normalize_image(
        Some("./Dockerfile"),
        RuntimeBackend::Pyxis,
        tmpdir.path(),
        "svc",
    )
    .expect_err("local path");
    assert!(
        err.to_string()
            .contains("Dockerfiles and build contexts are not supported")
    );

    let mount = normalize_mount("./data:/data", tmpdir.path()).expect("mount");
    assert!(mount.contains("/data"));
    assert_eq!(
        resolve_path("relative/path", tmpdir.path()).expect("resolve"),
        tmpdir.path().join("relative/path")
    );
    assert_eq!(
        crate::path_util::normalize_path(PathBuf::from("/tmp/a/./b/../c")),
        PathBuf::from("/tmp/a/c")
    );
}

#[test]
fn resolve_cache_dir_and_existing_path_cover_defaults_and_failures() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let resolved = resolve_cache_dir(&SlurmConfig::default(), tmpdir.path(), None).expect("cache");
    assert!(resolved.ends_with(".cache/hpc-compose"));

    let settings_default = resolve_cache_dir(
        &SlurmConfig::default(),
        tmpdir.path(),
        Some(Path::new("/shared/settings-cache")),
    )
    .expect("settings cache");
    assert_eq!(settings_default, Path::new("/shared/settings-cache"));

    let explicit = resolve_cache_dir(
        &SlurmConfig {
            cache_dir: Some("./cache".into()),
            ..SlurmConfig::default()
        },
        tmpdir.path(),
        Some(Path::new("/shared/settings-cache")),
    )
    .expect("explicit");
    assert_eq!(explicit, tmpdir.path().join("cache"));

    assert_eq!(
        normalize_existing_path(&compose).expect("existing"),
        compose.canonicalize().expect("canon")
    );
    let err = normalize_existing_path(&tmpdir.path().join("missing.yaml")).expect_err("missing");
    assert!(err.to_string().contains("failed to canonicalize"));
}

#[test]
fn build_plan_rejects_service_healthy_without_readiness() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "app".into(),
                ServiceSpec {
                    depends_on: DependsOnSpec::Map(BTreeMap::from([(
                        "redis".into(),
                        DependsOnConditionSpec {
                            condition: Some("service_healthy".into()),
                        },
                    )])),
                    ..service("redis:7")
                },
            ),
            ("redis".into(), service("redis:7")),
        ]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let err = build_plan(&compose, spec).expect_err("missing readiness");
    assert!(err.to_string().contains("service_healthy"));
    assert!(err.to_string().contains("does not define readiness"));
}

#[test]
fn build_plan_preserves_dependency_conditions() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "app".into(),
                ServiceSpec {
                    depends_on: DependsOnSpec::Map(BTreeMap::from([
                        (
                            "cache".into(),
                            DependsOnConditionSpec {
                                condition: Some("service_started".into()),
                            },
                        ),
                        (
                            "redis".into(),
                            DependsOnConditionSpec {
                                condition: Some("service_healthy".into()),
                            },
                        ),
                    ])),
                    ..service("redis:7")
                },
            ),
            (
                "redis".into(),
                ServiceSpec {
                    readiness: Some(ReadinessSpec::Log {
                        pattern: "ready".into(),
                        timeout_seconds: Some(5),
                    }),
                    ..service("redis:7")
                },
            ),
            ("cache".into(), service("redis:7")),
        ]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let plan = build_plan(&compose, spec).expect("plan");
    assert_eq!(
        plan.ordered_services
            .last()
            .expect("app")
            .depends_on
            .iter()
            .map(|dep| (&dep.name, dep.condition))
            .collect::<Vec<_>>(),
        vec![
            (&"cache".to_string(), DependencyCondition::ServiceStarted),
            (&"redis".to_string(), DependencyCondition::ServiceHealthy),
        ]
    );
}

#[test]
fn build_plan_normalizes_failure_policy_defaults_and_overrides() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            ("default".into(), service("redis:7")),
            (
                "restart-defaults".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: None,
                            backoff_seconds: None,
                            window_seconds: None,
                            max_restarts_in_window: None,
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "restart-custom".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(7),
                            backoff_seconds: Some(9),
                            window_seconds: Some(11),
                            max_restarts_in_window: Some(4),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "ignore".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::Ignore,
                            max_restarts: None,
                            backoff_seconds: None,
                            window_seconds: None,
                            max_restarts_in_window: None,
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
        ]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let plan = build_plan(&compose, spec).expect("plan");
    let by_name = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.as_str(), service.failure_policy.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_name.get("default"),
        Some(&ServiceFailurePolicy::default())
    );
    assert_eq!(
        by_name.get("restart-defaults"),
        Some(&ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 3,
            backoff_seconds: 5,
            window_seconds: 60,
            max_restarts_in_window: 3,
        })
    );
    assert_eq!(
        by_name.get("restart-custom"),
        Some(&ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 7,
            backoff_seconds: 9,
            window_seconds: 11,
            max_restarts_in_window: 4,
        })
    );
    assert_eq!(
        by_name.get("ignore"),
        Some(&ServiceFailurePolicy {
            mode: ServiceFailureMode::Ignore,
            max_restarts: 0,
            backoff_seconds: 0,
            window_seconds: 0,
            max_restarts_in_window: 0,
        })
    );
}

#[test]
fn build_plan_applies_partial_restart_window_overrides() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "window-seconds-only".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(4),
                            backoff_seconds: Some(9),
                            window_seconds: Some(30),
                            max_restarts_in_window: None,
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
            (
                "window-count-only".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(6),
                            backoff_seconds: Some(7),
                            window_seconds: None,
                            max_restarts_in_window: Some(2),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
        ]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let plan = build_plan(&compose, spec).expect("plan");
    let by_name = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.as_str(), service.failure_policy.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_name.get("window-seconds-only"),
        Some(&ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 4,
            backoff_seconds: 9,
            window_seconds: 30,
            max_restarts_in_window: 4,
        })
    );
    assert_eq!(
        by_name.get("window-count-only"),
        Some(&ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 6,
            backoff_seconds: 7,
            window_seconds: 60,
            max_restarts_in_window: 2,
        })
    );
}

#[test]
fn build_plan_rejects_invalid_failure_policy_combinations() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");

    let invalid_non_restart = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    failure_policy: Some(ServiceFailurePolicySpec {
                        mode: ServiceFailureMode::FailJob,
                        max_restarts: Some(2),
                        backoff_seconds: None,
                        window_seconds: None,
                        max_restarts_in_window: Some(1),
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, invalid_non_restart).expect_err("invalid fail_job policy");
    assert!(
        err.to_string()
            .contains("only valid when mode is restart_on_failure")
    );

    let invalid_restart = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    failure_policy: Some(ServiceFailurePolicySpec {
                        mode: ServiceFailureMode::RestartOnFailure,
                        max_restarts: Some(0),
                        backoff_seconds: Some(5),
                        window_seconds: Some(10),
                        max_restarts_in_window: Some(2),
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, invalid_restart).expect_err("invalid restart policy");
    assert!(err.to_string().contains("max_restarts"));

    let invalid_window = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    failure_policy: Some(ServiceFailurePolicySpec {
                        mode: ServiceFailureMode::RestartOnFailure,
                        max_restarts: Some(2),
                        backoff_seconds: Some(5),
                        window_seconds: Some(0),
                        max_restarts_in_window: Some(1),
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, invalid_window).expect_err("invalid restart window");
    assert!(err.to_string().contains("window_seconds"));

    let invalid_window_count = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([(
            "app".into(),
            ServiceSpec {
                slurm: ServiceSlurmConfig {
                    failure_policy: Some(ServiceFailurePolicySpec {
                        mode: ServiceFailureMode::RestartOnFailure,
                        max_restarts: Some(2),
                        backoff_seconds: Some(5),
                        window_seconds: Some(10),
                        max_restarts_in_window: Some(0),
                    }),
                    ..ServiceSlurmConfig::default()
                },
                ..service("redis:7")
            },
        )]),
    };
    let err = build_plan(&compose, invalid_window_count).expect_err("invalid restart window count");
    assert!(err.to_string().contains("max_restarts_in_window"));
}

#[test]
fn build_plan_rejects_dependencies_on_ignore_services() {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        runtime: RuntimeConfig::default(),
        name: Some("demo".into()),
        slurm: SlurmConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        sweep: None,
        services: BTreeMap::from([
            (
                "app".into(),
                ServiceSpec {
                    depends_on: DependsOnSpec::List(vec!["sidecar".into()]),
                    ..service("redis:7")
                },
            ),
            (
                "sidecar".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::Ignore,
                            max_restarts: None,
                            backoff_seconds: None,
                            window_seconds: None,
                            max_restarts_in_window: None,
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            ),
        ]),
    };
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("write");
    let err = build_plan(&compose, spec).expect_err("ignore dependency");
    assert!(err.to_string().contains("cannot be depended on"));
}

#[test]
fn pyxis_backend_rejects_local_sif_image() {
    let err = normalize_image(
        Some("./image.sif"),
        RuntimeBackend::Pyxis,
        Path::new("/tmp/project"),
        "app",
    )
    .expect_err("pyxis must reject a local .sif image");
    let msg = err.to_string();
    assert!(
        msg.contains("runtime.backend=pyxis expects a remote image"),
        "unexpected: {msg}"
    );
}

#[test]
fn mount_rejects_unsupported_mode() {
    let err = normalize_mount("./data:/data:rx", Path::new("/tmp/project"))
        .expect_err("unsupported mode rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported mode") && msg.contains("use ro or rw"),
        "unexpected: {msg}"
    );
}

#[test]
fn mount_rejects_relative_container_path() {
    let err = normalize_mount("./data:relative/path", Path::new("/tmp/project"))
        .expect_err("relative container path rejected");
    assert!(
        err.to_string().contains("container path must be absolute"),
        "unexpected: {err}"
    );
}

#[test]
fn mount_rejects_empty_components() {
    let err =
        normalize_mount(" :/data", Path::new("/tmp/project")).expect_err("empty host rejected");
    assert!(
        err.to_string()
            .contains("non-empty host and container paths"),
        "unexpected: {err}"
    );
}

#[test]
fn registry_host_handles_colon_host_without_dot() {
    assert_eq!(
        registry_host_for_remote("docker://myhost:5000/app:latest"),
        "myhost:5000"
    );
}

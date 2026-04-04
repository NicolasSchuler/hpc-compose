use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};

const ROOT_ALLOWED_KEYS: &[&str] = &["name", "services", "version", "x-slurm"];
const SERVICE_ALLOWED_KEYS: &[&str] = &[
    "image",
    "command",
    "entrypoint",
    "environment",
    "volumes",
    "working_dir",
    "depends_on",
    "readiness",
    "x-slurm",
    "x-enroot",
];

#[derive(Debug, Clone, Deserialize)]
pub struct ComposeSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "x-slurm", default)]
    pub slurm: SlurmConfig,
    pub services: BTreeMap<String, ServiceSpec>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmConfig {
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub qos: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub cpus_per_task: Option<u32>,
    #[serde(default)]
    pub mem: Option<String>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub gpus: Option<u32>,
    #[serde(default)]
    pub constraint: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub chdir: Option<String>,
    #[serde(default)]
    pub cache_dir: Option<String>,
    #[serde(default)]
    pub setup: Vec<String>,
    #[serde(default)]
    pub submit_args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceSpec {
    pub image: String,
    #[serde(default)]
    pub command: Option<CommandSpec>,
    #[serde(default)]
    pub entrypoint: Option<CommandSpec>,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(rename = "working_dir", default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub depends_on: DependsOnSpec,
    #[serde(default)]
    pub readiness: Option<ReadinessSpec>,
    #[serde(rename = "x-slurm", default)]
    pub slurm: ServiceSlurmConfig,
    #[serde(rename = "x-enroot", default)]
    pub enroot: ServiceEnrootConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSlurmConfig {
    #[serde(default)]
    pub cpus_per_task: Option<u32>,
    #[serde(default)]
    pub gpus: Option<u32>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub extra_srun_args: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrootConfig {
    #[serde(default)]
    pub prepare: Option<PrepareSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareSpec {
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub mounts: Vec<String>,
    #[serde(default)]
    pub env: EnvironmentSpec,
    #[serde(default = "default_true")]
    pub root: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
pub enum DependsOnSpec {
    #[default]
    None,
    List(Vec<String>),
    Map(BTreeMap<String, DependsOnConditionSpec>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependsOnConditionSpec {
    #[serde(default)]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
pub enum EnvironmentSpec {
    #[default]
    None,
    Map(BTreeMap<String, String>),
    List(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CommandSpec {
    String(String),
    Vec(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReadinessSpec {
    Sleep {
        seconds: u64,
    },
    Tcp {
        port: u16,
        #[serde(default)]
        host: Option<String>,
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
    Log {
        pattern: String,
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
}

impl ComposeSpec {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read spec at {}", path.display()))?;
        let value: Value = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse YAML at {}", path.display()))?;
        validate_root(&value)?;
        let spec: ComposeSpec = serde_yaml::from_value(value)
            .with_context(|| format!("failed to deserialize spec at {}", path.display()))?;
        Ok(spec)
    }
}

impl DependsOnSpec {
    pub fn names(&self) -> Result<Vec<String>> {
        match self {
            DependsOnSpec::None => Ok(Vec::new()),
            DependsOnSpec::List(items) => Ok(items.clone()),
            DependsOnSpec::Map(items) => {
                let mut out = Vec::with_capacity(items.len());
                for (name, cfg) in items {
                    if let Some(condition) = &cfg.condition
                        && condition != "service_started"
                    {
                        bail!(
                            "depends_on condition for service '{name}' must be 'service_started'; readiness gates are configured separately"
                        );
                    }
                    out.push(name.clone());
                }
                Ok(out)
            }
        }
    }
}

impl EnvironmentSpec {
    pub fn to_pairs(&self) -> Result<Vec<(String, String)>> {
        match self {
            EnvironmentSpec::None => Ok(Vec::new()),
            EnvironmentSpec::Map(map) => Ok(map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()),
            EnvironmentSpec::List(items) => items
                .iter()
                .map(|item| {
                    let Some((key, value)) = item.split_once('=') else {
                        bail!("environment list items must use KEY=VALUE syntax");
                    };
                    Ok((key.to_string(), value.to_string()))
                })
                .collect(),
        }
    }
}

impl CommandSpec {
    pub fn is_string(&self) -> bool {
        matches!(self, CommandSpec::String(_))
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            CommandSpec::String(value) => Some(value),
            CommandSpec::Vec(_) => None,
        }
    }

    pub fn as_vec(&self) -> Option<&[String]> {
        match self {
            CommandSpec::String(_) => None,
            CommandSpec::Vec(value) => Some(value),
        }
    }
}

fn default_true() -> bool {
    true
}

fn validate_root(value: &Value) -> Result<()> {
    let Some(root) = value.as_mapping() else {
        bail!("top-level YAML document must be a mapping");
    };
    validate_mapping_keys("root", root, ROOT_ALLOWED_KEYS)?;
    let Some(services) = root.get(Value::String("services".into())) else {
        bail!("spec must contain a top-level 'services' mapping");
    };
    let Some(service_map) = services.as_mapping() else {
        bail!("'services' must be a mapping");
    };
    for (name, service) in service_map {
        let Some(service_name) = name.as_str() else {
            bail!("service names must be strings");
        };
        let Some(service_mapping) = service.as_mapping() else {
            bail!("service '{service_name}' must be a mapping");
        };
        validate_mapping_keys(
            &format!("service '{service_name}'"),
            service_mapping,
            SERVICE_ALLOWED_KEYS,
        )?;
    }
    Ok(())
}

fn validate_mapping_keys(scope: &str, mapping: &Mapping, allowed: &[&str]) -> Result<()> {
    for key in mapping.keys() {
        let Some(key_name) = key.as_str() else {
            bail!("{scope} contains a non-string key");
        };
        if allowed.contains(&key_name) {
            continue;
        }
        let message = match key_name {
            "build" => {
                "build is not supported in v1; use image: plus x-enroot.prepare to customize an Enroot image before submission"
            }
            "ports" => {
                "ports are not supported; use host-network semantics and explicit readiness checks"
            }
            "networks" | "network_mode" => {
                "custom container networking is not supported under this Slurm/Enroot execution model"
            }
            "restart" => "restart policies are not supported inside a batch job",
            "deploy" => {
                "deploy is not supported; this tool targets one Slurm allocation, not a long-running orchestrator"
            }
            other => bail!("{scope} uses unsupported key '{other}'"),
        };
        bail!("{scope}: {message}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write_spec(tmpdir: &Path, body: &str) -> std::path::PathBuf {
        let path = tmpdir.join("compose.yaml");
        fs::write(&path, body).expect("write compose");
        path
    }

    #[test]
    fn load_minimal_spec_uses_defaults() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
name: demo
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(spec.name.as_deref(), Some("demo"));
        assert_eq!(spec.services.len(), 1);
        assert!(spec.slurm.cache_dir.is_none());
        let service = spec.services.get("app").expect("service");
        assert!(service.command.is_none());
        assert!(service.volumes.is_empty());
    }

    #[test]
    fn rejects_build_with_actionable_message() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    build: .
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("build is not supported in v1"));
        assert!(err.to_string().contains("x-enroot.prepare"));
    }

    #[test]
    fn rejects_ports_with_actionable_message() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    ports:
      - "6379:6379"
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("ports are not supported"));
    }

    #[test]
    fn rejects_unknown_service_key() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    mystery: true
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("unsupported key 'mystery'"));
    }

    #[test]
    fn rejects_non_mapping_root() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(tmpdir.path(), "- not-a-mapping\n");
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("top-level YAML document must be a mapping")
        );
    }

    #[test]
    fn rejects_missing_services() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(tmpdir.path(), "name: demo\n");
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("top-level 'services'"));
    }

    #[test]
    fn environment_list_requires_key_value_pairs() {
        let env = EnvironmentSpec::List(vec!["GOOD=1".into(), "BROKEN".into()]);
        let err = env.to_pairs().expect_err("should fail");
        assert!(err.to_string().contains("KEY=VALUE"));
    }

    #[test]
    fn depends_on_map_rejects_unsupported_condition() {
        let deps = DependsOnSpec::Map(BTreeMap::from([(
            "redis".into(),
            DependsOnConditionSpec {
                condition: Some("service_healthy".into()),
            },
        )]));
        let err = deps.names().expect_err("should fail");
        assert!(err.to_string().contains("service_started"));
    }

    #[test]
    fn depends_on_map_accepts_service_started() {
        let deps = DependsOnSpec::Map(BTreeMap::from([(
            "redis".into(),
            DependsOnConditionSpec {
                condition: Some("service_started".into()),
            },
        )]));
        assert_eq!(deps.names().expect("names"), vec!["redis"]);
    }

    #[test]
    fn command_accessors_match_variants() {
        let string_cmd = CommandSpec::String("echo hi".into());
        assert!(string_cmd.is_string());
        assert_eq!(string_cmd.as_string(), Some("echo hi"));
        assert!(string_cmd.as_vec().is_none());

        let vec_cmd = CommandSpec::Vec(vec!["python".into(), "-m".into(), "main".into()]);
        assert!(!vec_cmd.is_string());
        assert!(vec_cmd.as_string().is_none());
        assert_eq!(
            vec_cmd.as_vec(),
            Some(&["python".to_string(), "-m".to_string(), "main".to_string()][..])
        );
    }

    #[test]
    fn readiness_variants_deserialize() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  tcp:
    image: redis:7
    readiness:
      type: tcp
      port: 6379
      host: 127.0.0.1
      timeout_seconds: 30
  log:
    image: redis:7
    readiness:
      type: log
      pattern: ready
      timeout_seconds: 10
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(
            spec.services
                .get("tcp")
                .and_then(|svc| svc.readiness.clone()),
            Some(ReadinessSpec::Tcp {
                port: 6379,
                host: Some("127.0.0.1".into()),
                timeout_seconds: Some(30),
            })
        );
        assert_eq!(
            spec.services
                .get("log")
                .and_then(|svc| svc.readiness.clone()),
            Some(ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: Some(10),
            })
        );
    }

    #[test]
    fn parse_and_structure_errors_cover_remaining_validation_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");

        let invalid_yaml = write_spec(tmpdir.path(), "services: [\n");
        let err = ComposeSpec::load(&invalid_yaml).expect_err("invalid yaml");
        assert!(err.to_string().contains("failed to parse YAML"));

        let non_mapping_services = write_spec(
            tmpdir.path(),
            r#"
services:
  - app
"#,
        );
        let err = ComposeSpec::load(&non_mapping_services).expect_err("services mapping");
        assert!(err.to_string().contains("'services' must be a mapping"));

        let non_mapping_service = write_spec(
            tmpdir.path(),
            r#"
services:
  app: hello
"#,
        );
        let err = ComposeSpec::load(&non_mapping_service).expect_err("service mapping");
        assert!(err.to_string().contains("service 'app' must be a mapping"));

        let root_unknown = write_spec(
            tmpdir.path(),
            r#"
version: "3"
unknown: true
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&root_unknown).expect_err("root unknown");
        assert!(
            err.to_string()
                .contains("root uses unsupported key 'unknown'")
        );

        let networks = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    networks: [default]
"#,
        );
        let err = ComposeSpec::load(&networks).expect_err("networks");
        assert!(err.to_string().contains("custom container networking"));

        let restart = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    restart: always
"#,
        );
        let err = ComposeSpec::load(&restart).expect_err("restart");
        assert!(
            err.to_string()
                .contains("restart policies are not supported")
        );

        let deploy = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    deploy: {}
"#,
        );
        let err = ComposeSpec::load(&deploy).expect_err("deploy");
        assert!(err.to_string().contains("long-running orchestrator"));
    }

    #[test]
    fn environment_map_and_command_defaults_cover_remaining_helpers() {
        let env = EnvironmentSpec::Map(BTreeMap::from([("A".into(), "B".into())]));
        assert_eq!(
            env.to_pairs().expect("pairs"),
            vec![("A".into(), "B".into())]
        );
        assert!(default_true());
    }

    #[test]
    fn deserialize_and_key_type_errors_cover_last_branches() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");

        let bad_image_type = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: [redis:7]
"#,
        );
        let err = ComposeSpec::load(&bad_image_type).expect_err("deserialize");
        assert!(err.to_string().contains("failed to deserialize"));

        let numeric_service_name = write_spec(
            tmpdir.path(),
            r#"
services:
  1:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&numeric_service_name).expect_err("non-string service");
        assert!(err.to_string().contains("service names must be strings"));

        let non_string_root_key = write_spec(
            tmpdir.path(),
            r#"
1: true
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&non_string_root_key).expect_err("non-string key");
        assert!(err.to_string().contains("root contains a non-string key"));
    }
}

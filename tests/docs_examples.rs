use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn example_yaml_files() -> Vec<String> {
    let mut files = fs::read_dir(repo_root().join("examples"))
        .expect("read examples directory")
        .filter_map(|entry| {
            let path: PathBuf = entry.expect("read examples entry").path();
            let is_yaml = path.extension().and_then(|ext| ext.to_str()) == Some("yaml");
            is_yaml.then(|| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .expect("example filename should be UTF-8")
                    .to_string()
            })
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn examples_guide_mentions_every_repository_yaml_example() {
    let examples_guide =
        fs::read_to_string(repo_root().join("docs/src/examples.md")).expect("read examples guide");
    let example_source = fs::read_to_string(repo_root().join("docs/src/example-source.md"))
        .expect("read example source appendix");

    for file in example_yaml_files() {
        assert!(
            examples_guide.contains(&file),
            "docs/src/examples.md should mention examples/{file}"
        );
        assert!(
            example_source.contains(&format!("../../examples/{file}")),
            "docs/src/example-source.md should include examples/{file}"
        );
    }
}

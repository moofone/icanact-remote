use std::fs;
use std::path::Path;

#[test]
fn forbidden_direct_dependencies_and_error_erasure_stay_absent() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");
    let erased_error_crate = concat!("any", "how");

    for dependency in [erased_error_crate, "rmp-serde", "uuid"] {
        let declaration = format!("{dependency} =");
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(&declaration)),
            "forbidden direct dependency `{dependency}` reintroduced"
        );
    }

    let erased_error_path = format!("{erased_error_crate}::");
    for source_root in ["src", "examples", "tests", "benches"] {
        assert_no_source_reference(
            &root.join(source_root),
            &erased_error_path,
            Path::new(file!()),
        );
    }
}

fn assert_no_source_reference(root: &Path, needle: &str, this_test: &Path) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in
            fs::read_dir(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        {
            let path = entry.expect("read directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if is_rust_source(&path) && !path.ends_with(this_test) {
                let source = fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
                assert!(
                    !source.contains(needle),
                    "erased error handling is forbidden in {}",
                    path.display()
                );
            }
        }
    }
}

fn is_rust_source(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "rs")
}

use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn tcp_only_transport_guardrail_no_udp_socket_usage() {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let forbidden = [
        "std::net::UdpSocket",
        "tokio::net::UdpSocket",
        "UdpSocket::bind",
    ];
    let mut violations = Vec::new();

    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };
        for (idx, line) in content.lines().enumerate() {
            if forbidden.iter().any(|needle| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", file.display(), idx + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "UDP usage violates TCP-only scope lock:\n{}",
        violations.join("\n")
    );
}

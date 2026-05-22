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
fn udp_transport_guardrail_udp_socket_usage_is_scoped() {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let allowed_files = [
        "src/ask_responder.rs",
        "src/connection_pool.rs",
        "src/handle.rs",
    ];
    let mut udp_usages = Vec::new();
    let mut violations = Vec::new();

    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };
        for (idx, line) in content.lines().enumerate() {
            if line.contains("tokio::net::UdpSocket") || line.contains("UdpSocket::from_std") {
                let display = file.display().to_string();
                udp_usages.push(format!("{display}:{}: {}", idx + 1, line.trim()));
                if !allowed_files
                    .iter()
                    .any(|allowed| display.ends_with(allowed))
                {
                    violations.push(format!("{display}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }

    assert!(
        !udp_usages.is_empty(),
        "expected UDP socket usage for UDP transport, but found none"
    );
    assert!(
        violations.is_empty(),
        "UDP socket usage must stay scoped to transport runtime files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn remote_invariants_are_present_and_complete() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("docs/architecture/REMOTE_INNVARIANTS.md");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} must exist: {error}", path.display()));

    assert!(
        contents.contains("Implementation status: `UNPROVED`"),
        "the architecture contract must not claim unverified conformance"
    );

    let headings = [
        "### REMOTE-1 — One authoritative owner per plane",
        "### REMOTE-2 — Messaging hot paths pay no incidental tax",
        "### REMOTE-3 — Evidence never widens in scope",
        "### REMOTE-4 — Stale work cannot mutate successor state",
        "### REMOTE-5 — All asynchronous work is bounded and overload remains local",
    ];
    for heading in headings {
        assert_eq!(
            contents.matches(heading).count(),
            1,
            "{heading} must appear exactly once"
        );
    }
    assert_eq!(
        contents.matches("### REMOTE-").count(),
        headings.len(),
        "the canonical contract is intentionally limited to five invariants"
    );

    for required in [
        "icanact-core",
        "SWIM",
        "SocketAddr",
        "PeerId",
        "session generation",
        "allocation",
        "backpressure",
        "fail-first",
    ] {
        assert!(
            contents.contains(required),
            "architecture contract must pin required boundary term {required:?}"
        );
    }

    assert!(
        include_str!("../README.md").contains("docs/architecture/REMOTE_INNVARIANTS.md"),
        "README must link the canonical remote architecture contract"
    );
}

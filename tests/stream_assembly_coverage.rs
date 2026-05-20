use icanact_remote::{GossipConfig, KeyPair, StreamHeader, registry::GossipRegistry};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::test]
async fn stream_assembly_copies_chunks_into_buffer() {
    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("stream_assembly_coverage")),
        ..Default::default()
    };

    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let registry = GossipRegistry::<()>::new(bind_addr, config);

    let header0 = StreamHeader {
        stream_id: 42,
        total_size: 6,
        chunk_size: 3,
        chunk_index: 0,
        type_hash: 0,
        actor_id: 0,
    };

    registry.start_stream_assembly(header0, None, None).await;
    registry.add_stream_chunk(header0, vec![1, 2, 3]).await;

    let header1 = StreamHeader {
        chunk_index: 1,
        ..header0
    };
    registry.add_stream_chunk(header1, vec![4, 5, 6]).await;

    let result = registry
        .complete_stream_assembly(header0.stream_id)
        .await
        .expect("stream assembly should complete");

    assert_eq!(result.data.as_ref(), &[1, 2, 3, 4, 5, 6]);
}

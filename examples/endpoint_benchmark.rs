use grpc_client::proto::geyser::{CommitmentLevel, SubscribeRequestFilterSlots};
use grpc_client::{
    BenchmarkConfig, BenchmarkEndpoint, GrpcClient, SubscribeRequestBuilder,
    spawn_subscribe_benchmark,
};

fn build_client(endpoint: &str) -> Result<GrpcClient, Box<dyn std::error::Error>> {
    let mut builder = GrpcClient::builder_from_shared(endpoint.to_owned())?
        .tcp_nodelay(true)
        .max_decoding_message_size(1024 * 1024 * 1024);
    if let Ok(token) = std::env::var("X_TOKEN") {
        builder = builder.x_token(token)?;
    }
    Ok(builder.build())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::args().skip(1).collect::<Vec<_>>();
    if endpoints.is_empty() {
        eprintln!("usage: endpoint_benchmark <endpoint> [endpoint...]");
        return Ok(());
    }

    let benchmark_endpoints = endpoints
        .iter()
        .map(|endpoint| {
            Ok(BenchmarkEndpoint::new(
                endpoint.clone(),
                build_client(endpoint)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    let request = SubscribeRequestBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .add_slot_filter(
            "bench",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )
        .build();

    let mut handle = spawn_subscribe_benchmark(
        benchmark_endpoints,
        request,
        BenchmarkConfig {
            min_samples_for_winner: 8,
            ..BenchmarkConfig::default()
        },
    );

    loop {
        handle.changed().await?;
        let snapshot = handle.snapshot();
        if let Some(winner) = snapshot.winner() {
            println!(
                "winner={} samples={} first_update={:?} avg_created_at_lag={:?} last_slot={:?}",
                winner.name,
                winner.samples,
                winner.first_update_latency,
                winner.avg_created_at_lag,
                winner.last_observed_slot,
            );
        }
        for score in &snapshot.scores {
            println!(
                "endpoint={} samples={} first_update={:?} avg_created_at_lag={:?} errors={} dropped_reports={}",
                score.name,
                score.samples,
                score.first_update_latency,
                score.avg_created_at_lag,
                score.transient_errors,
                score.dropped_reports,
            );
        }
        println!();
    }
}

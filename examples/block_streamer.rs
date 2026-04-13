use grpc_client::proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterBlocks, subscribe_update::UpdateOneof,
};
use grpc_client::{GrpcClient, SubscribeRequestBuilder};
use tokio_stream::StreamExt;

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
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:10000".to_string());
    let include_account = args.get(2).cloned();

    let client = build_client(&endpoint)?;
    let request = SubscribeRequestBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .add_block_filter(
            "blocks",
            SubscribeRequestFilterBlocks {
                account_include: include_account.into_iter().collect(),
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        )
        .build();

    let (_controller, mut stream) = client.subscribe_with_request(request);

    while let Some(item) = stream.next().await {
        match item {
            Ok(update) => {
                if let Some(UpdateOneof::Block(block)) = update.update_oneof {
                    println!(
                        "slot={} blockhash={} parent_slot={} txs={} accounts={} entries={}",
                        block.slot,
                        block.blockhash,
                        block.parent_slot,
                        block.transactions.len(),
                        block.accounts.len(),
                        block.entries.len(),
                    );
                }
            }
            Err(error) => eprintln!("block stream error: {error}"),
        }
    }

    Ok(())
}

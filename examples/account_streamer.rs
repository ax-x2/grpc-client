use grpc_client::proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterAccounts, subscribe_update::UpdateOneof,
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

fn base58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "http://localhost:10001".to_string());
    let owner = args.get(2).cloned().unwrap_or_default();

    let client = build_client(&endpoint)?;
    let request = SubscribeRequestBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .add_account_filter(
            "accounts",
            SubscribeRequestFilterAccounts {
                account: Vec::new(),
                owner: if owner.is_empty() {
                    Vec::new()
                } else {
                    vec![owner]
                },
                filters: Vec::new(),
                nonempty_txn_signature: None,
            },
        )
        .build();

    let (_controller, mut stream) = client.subscribe_with_request(request);

    while let Some(item) = stream.next().await {
        match item {
            Ok(update) => {
                if let Some(UpdateOneof::Account(account)) = update.update_oneof {
                    if let Some(info) = account.account {
                        println!(
                            "slot={} pubkey={} owner={} lamports={} data_len={} startup={}",
                            account.slot,
                            base58(&info.pubkey),
                            base58(&info.owner),
                            info.lamports,
                            info.data.len(),
                            account.is_startup,
                        );
                    }
                }
            }
            Err(error) => eprintln!("account stream error: {error}"),
        }
    }

    Ok(())
}

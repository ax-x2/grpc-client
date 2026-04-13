use grpc_client::proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof,
};
use grpc_client::{GrpcClient, SubscribeRequestBuilder};
use std::time::Duration;
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

fn optional_arg(args: &[String], index: usize) -> Option<String> {
    args.get(index)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn tx_filter(address: Option<String>) -> SubscribeRequestFilterTransactions {
    SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        signature: None,
        account_include: address.into_iter().collect(),
        account_exclude: Vec::new(),
        account_required: Vec::new(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:10000".to_string());
    let first_target = optional_arg(&args, 2);
    let second_target = optional_arg(&args, 3);

    let client = build_client(&endpoint)?;
    let request = SubscribeRequestBuilder::new()
        .commitment(CommitmentLevel::Processed)
        .add_transaction_filter("transactions", tx_filter(first_target))
        .build();

    let (controller, mut stream) = client.subscribe_with_request(request);
    let switch_timer = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(switch_timer);
    let mut switched = false;

    loop {
        tokio::select! {
            _ = &mut switch_timer, if !switched && second_target.is_some() => {
                let mut batch = controller.batch();
                batch.request_mut().transactions.clear();
                batch.request_mut().transactions.insert(
                    "transactions".to_string(),
                    tx_filter(second_target.clone()),
                );
                batch.commit();
                switched = true;
                println!("updated transaction filter on the live stream without reconnecting");
            }
            item = stream.next() => {
                let Some(item) = item else {
                    break;
                };
                match item {
                    Ok(update) => {
                        if let Some(UpdateOneof::Transaction(transaction)) = update.update_oneof {
                            if let Some(info) = transaction.transaction {
                                println!(
                                    "slot={} signature={} vote={} index={}",
                                    transaction.slot,
                                    base58(&info.signature),
                                    info.is_vote,
                                    info.index,
                                );
                            }
                        }
                    }
                    Err(error) => eprintln!("transaction stream error: {error}"),
                }
            }
        }
    }

    Ok(())
}

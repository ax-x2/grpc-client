use grpc_client::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof,
};
use grpc_client::{GrpcClient, SubscribeRequestBuilder, SubscriptionController};
use std::time::Duration;
use tokio_stream::StreamExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Accounts,
    Transactions,
}

impl Mode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "accounts" => Some(Self::Accounts),
            "transactions" => Some(Self::Transactions),
            _ => None,
        }
    }
}

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

fn usage(binary: &str) {
    eprintln!(
        "usage: {binary} <endpoint> <accounts|transactions> <first_filter> <second_filter> [switch_after_secs]"
    );
    eprintln!(
        "accounts mode uses exact account pubkeys; transactions mode uses account_include pubkeys"
    );
}

fn build_request(mode: Mode, filter_value: &str) -> SubscribeRequest {
    match mode {
        Mode::Accounts => SubscribeRequestBuilder::new()
            .commitment(CommitmentLevel::Processed)
            .add_account_filter(
                "live-accounts",
                SubscribeRequestFilterAccounts {
                    account: vec![filter_value.to_string()],
                    owner: Vec::new(),
                    filters: Vec::new(),
                    nonempty_txn_signature: None,
                },
            )
            .build(),
        Mode::Transactions => SubscribeRequestBuilder::new()
            .commitment(CommitmentLevel::Processed)
            .add_transaction_filter(
                "live-transactions",
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include: vec![filter_value.to_string()],
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                },
            )
            .build(),
    }
}

fn apply_filter_update(
    controller: &SubscriptionController<SubscribeRequest>,
    mode: Mode,
    filter_value: &str,
) {
    controller.modify(|request| match mode {
        Mode::Accounts => {
            request.accounts.clear();
            request.accounts.insert(
                "live-accounts".to_string(),
                SubscribeRequestFilterAccounts {
                    account: vec![filter_value.to_string()],
                    owner: Vec::new(),
                    filters: Vec::new(),
                    nonempty_txn_signature: None,
                },
            );
        }
        Mode::Transactions => {
            request.transactions.clear();
            request.transactions.insert(
                "live-transactions".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include: vec![filter_value.to_string()],
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                },
            );
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let binary = args
        .first()
        .map(String::as_str)
        .unwrap_or("live_filter_update");

    let Some(endpoint) = args.get(1) else {
        usage(binary);
        return Ok(());
    };
    let Some(mode) = args.get(2).and_then(|value| Mode::parse(value)) else {
        usage(binary);
        return Ok(());
    };
    let Some(first_filter) = args.get(3) else {
        usage(binary);
        return Ok(());
    };
    let Some(second_filter) = args.get(4) else {
        usage(binary);
        return Ok(());
    };
    let switch_after = args
        .get(5)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10);

    let client = build_client(endpoint)?;
    let request = build_request(mode, first_filter);
    let (controller, mut stream) = client.subscribe_with_request(request);
    let switch_timer = tokio::time::sleep(Duration::from_secs(switch_after));
    tokio::pin!(switch_timer);
    let mut switched = false;

    println!(
        "opened one stream in {:?} mode, first filter={}, second filter={}, switch_after={}s",
        mode, first_filter, second_filter, switch_after
    );

    loop {
        tokio::select! {
            _ = &mut switch_timer, if !switched => {
                apply_filter_update(&controller, mode, second_filter);
                switched = true;
                println!(
                    "applied live filter update on the existing stream: {} -> {}",
                    first_filter, second_filter
                );
            }
            item = stream.next() => {
                let Some(item) = item else {
                    break;
                };
                match item {
                    Ok(update) => match update.update_oneof {
                        Some(UpdateOneof::Account(account)) if mode == Mode::Accounts => {
                            if let Some(info) = account.account {
                                println!(
                                    "account slot={} pubkey={} owner={} lamports={}",
                                    account.slot,
                                    base58(&info.pubkey),
                                    base58(&info.owner),
                                    info.lamports,
                                );
                            }
                        }
                        Some(UpdateOneof::Transaction(transaction)) if mode == Mode::Transactions => {
                            if let Some(info) = transaction.transaction {
                                println!(
                                    "transaction slot={} signature={} vote={} index={}",
                                    transaction.slot,
                                    base58(&info.signature),
                                    info.is_vote,
                                    info.index,
                                );
                            }
                        }
                        _ => {}
                    },
                    Err(error) => eprintln!("stream error: {error}"),
                }
            }
        }
    }

    Ok(())
}

#![allow(deprecated)]

use alloy_provider::ReqwestProvider;
use clap::Parser;
use pico_sdk::{client::DefaultProverClient, load_elf};
use rsp_client_executor::{io::ClientExecutorInput, ChainVariant, CHAIN_ID_ETH_MAINNET};
use rsp_host_executor::HostExecutor;
use std::path::PathBuf;
use tracing_subscriber::{
    filter::EnvFilter, fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
};
mod cli;
use cli::ProviderArgs;

/// The arguments for the host executable.
#[derive(Debug, Clone, Parser)]
struct HostArgs {
    /// The block number of the block to execute.
    #[clap(long)]
    block_number: u64,

    /// Provider configuration for RPC API
    #[clap(flatten)]
    provider: ProviderArgs,

    /// Where to dump the elf and stdin for the monolithic SP1 program.
    #[clap(long)]
    dump_dir: Option<PathBuf>,

    /// Optional path to the directory containing cached client input. A new cache file will be
    /// created from RPC data if it doesn't already exist.
    #[clap(long)]
    cache_dir: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> eyre::Result<()> {
    // Intialize the environment variables.
    dotenv::dotenv().ok();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    // Initialize the logger.
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    // Parse the command line arguments.
    let args = HostArgs::parse();
    let provider_config = args.provider.clone().into_provider().await?;
    assert_eq!(provider_config.chain_id, CHAIN_ID_ETH_MAINNET);
    let variant = ChainVariant::Ethereum;

    let client_input_from_cache = try_load_input_from_cache(
        args.cache_dir.as_ref(),
        provider_config.chain_id,
        args.block_number,
    )?;

    let client_input = match (
        client_input_from_cache,
        provider_config.basic_rpc_url,
        provider_config.debug_rpc_url,
    ) {
        (Some(client_input_from_cache), _, _) => client_input_from_cache,
        (None, Some(basic_rpc_url), Some(debug_rpc_url)) => {
            // Cache not found, but RPC is set.
            // Setup the provider.
            let basic_provider = ReqwestProvider::new_http(basic_rpc_url);
            let debug_provider = ReqwestProvider::new_http(debug_rpc_url);

            // Setup the host executor.
            let host_executor = HostExecutor::new(basic_provider, debug_provider);

            // Execute the host.
            let client_input = host_executor
                .execute_block(args.block_number, variant)
                .await
                .expect("failed to execute host");

            if let Some(ref cache_dir) = args.cache_dir {
                let input_folder = cache_dir.join(format!("input/{}", provider_config.chain_id));
                if !input_folder.exists() {
                    std::fs::create_dir_all(&input_folder)?;
                }

                let input_path = input_folder.join(format!("{}.bin", args.block_number));
                let mut cache_file = std::fs::File::create(input_path)?;

                bincode::serialize_into(&mut cache_file, &client_input)?;
            }

            client_input
        }
        _ => {
            eyre::bail!("cache not found and RPC URL not provided")
        }
    };

    // Generate the proof.
    let elf = load_elf("../../client-eth/elf/riscv32im-pico-zkvm-elf");
    let client = DefaultProverClient::new(&elf);

    // Execute the block inside the zkVM.
    let mut stdin_builder = client.new_stdin_builder();
    let buffer = bincode::serialize(&client_input).unwrap();
    stdin_builder.write_slice(&buffer);

    // Only execute the program.
    let (cycles, _pv_stream) = client.emulate(stdin_builder.clone());
    // let (_public_values, execution_report) = client.execute(&pk.elf, &stdin).run().unwrap();

    println!("rsp cycles: {}", cycles);

    if let Some(dump_dir) = args.dump_dir {
        let dump_dir = dump_dir.join(format!("{}", args.block_number));
        let elf_path = dump_dir.join("basic_elf.bin");
        let stdin_path = dump_dir.join("basic_stdin.bin");
        std::fs::write(elf_path, &elf)?;
        std::fs::write(stdin_path, bincode::serialize(&stdin_builder)?)?;
    }

    Ok(())
}

fn try_load_input_from_cache(
    cache_dir: Option<&PathBuf>,
    chain_id: u64,
    block_number: u64,
) -> eyre::Result<Option<ClientExecutorInput>> {
    Ok(if let Some(cache_dir) = cache_dir {
        let cache_path = cache_dir.join(format!("input/{}/{}.bin", chain_id, block_number));

        if cache_path.exists() {
            // Try to deserialize the cache file, but handle errors gracefully
            match std::fs::File::open(&cache_path) {
                Ok(mut cache_file) => match bincode::deserialize_from(&mut cache_file) {
                    Ok(client_input) => Some(client_input),
                    Err(err) => {
                        tracing::warn!(
                            "Failed to deserialize cache file at {}: {}",
                            cache_path.display(),
                            err
                        );
                        None
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        "Failed to open cache file at {}: {}",
                        cache_path.display(),
                        err
                    );
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    })
}

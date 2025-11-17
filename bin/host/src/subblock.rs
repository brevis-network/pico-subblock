//! Subblock executor.
//!
//! This is a standalone program that can be used to execute a subblock, and optionally dump the
//! elf/stdin pairs to a directory.

#![allow(deprecated)]

use alloy_provider::ReqwestProvider;
use clap::Parser;
use pico_sdk::{client::DefaultProverClient, init_logger, load_elf, HashableKey};
use rsp_client_executor::{
    io::{AggregationInput, SubblockHostOutput},
    ChainVariant, EthereumVariant,
};
use rsp_host_executor::HostExecutor;
use std::{
    env,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

mod cli;
use cli::ProviderArgs;

/// The arguments for the subblock executable.
#[derive(Debug, Clone, Parser)]
struct HostArgs {
    /// The block number of the block to execute.
    #[clap(long)]
    block_number: u64,
    #[clap(flatten)]
    provider: ProviderArgs,

    #[clap(long)]
    execute: bool,
    #[clap(long)]
    prove: bool,
    #[clap(long)]
    execution_witness: bool,

    /// Where to dump the elf and stdin for the subblock and aggregation programs.
    #[clap(long)]
    dump_dir: Option<PathBuf>,
    /// Optional path to the directory containing cached client input. A new cache file will be
    /// created from RPC data if it doesn't already exist.
    #[clap(long)]
    cache_dir: Option<PathBuf>,
}

fn resolve_dump_dir(dump_dir: Option<&PathBuf>, block_number: u64) -> PathBuf {
    let gas_segment = match env::var("SUBBLOCK_GAS_LIMIT").ok().and_then(|s| s.parse::<u64>().ok())
    {
        Some(g) => format!("gas{}", g),
        None => "gasUNSET".to_string(),
    };

    let base = dump_dir.cloned().unwrap_or_else(|| PathBuf::from("."));

    base.join(format!("block{}", block_number)).join(gas_segment)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();
    init_logger();

    let t_start = Instant::now();
    let t_client_input = Instant::now();

    // Parse the command line arguments.
    let args = HostArgs::parse();

    let provider_config = args.provider.clone().into_provider().await?;

    let cache_data = try_load_input_from_cache(
        args.cache_dir.as_ref(),
        provider_config.chain_id,
        args.block_number,
    )?;

    let client_input =
        match (cache_data, provider_config.basic_rpc_url, provider_config.debug_rpc_url) {
            (Some(cache_data), _, _) => cache_data,
            (None, Some(basic_rpc_url), Some(debug_rpc_url)) => {
                // Cache not found but we have RPC
                // Setup the provider.
                let basic_provider = ReqwestProvider::new_http(basic_rpc_url);
                let debug_provider = ReqwestProvider::new_http(debug_rpc_url);

                // Setup the host executor.
                let host_executor = HostExecutor::new(basic_provider, debug_provider);

                // Execute the host.
                let t_prepare_sb_stdin = Instant::now();
                let cache_data = host_executor
                    .execute_subblock(
                        args.execution_witness,
                        args.block_number,
                        ChainVariant::Ethereum,
                        args.dump_dir.clone(),
                    )
                    .await
                    .expect("failed to execute host");

                println!(
                    "TIMER_ALL preprocess subblocks stdin: {:.3?}",
                    t_prepare_sb_stdin.elapsed()
                );

                let t_write_to_cache = Instant::now();

                if let Some(ref cache_dir) = args.cache_dir {
                    let input_folder =
                        cache_dir.join(format!("input/{}", provider_config.chain_id));
                    if !input_folder.exists() {
                        std::fs::create_dir_all(&input_folder)?;
                    }

                    let input_path = input_folder.join(format!("{}.bin", args.block_number));
                    let mut cache_file = std::fs::File::create(input_path)?;

                    bincode::serialize_into(&mut cache_file, &cache_data)?;
                }

                println!("write_to_cache time: {:?}", t_write_to_cache.elapsed());

                cache_data
            }
            _ => {
                eyre::bail!("cache not found and RPC URL not provided")
            }
        };
    println!("TIMER_ALL t_client_input: {:.3?}", t_client_input.elapsed());

    let t_post_client_input = Instant::now();
    let t_setup_client = Instant::now();
    // Generate the proof.
    let subblock_elf = load_elf("./bin/client-eth-subblock/pico-elf/riscv32im-pico-zkvm-elf");
    let subblock_client = DefaultProverClient::new(&subblock_elf);
    let agg_elf = load_elf("./bin/client-eth-agg/pico-elf/riscv32im-pico-zkvm-elf");
    let agg_client = DefaultProverClient::new(&agg_elf);

    println!("TIMER_ALL t_setup_client: {:.3?}", t_setup_client.elapsed());

    schedule_subblock_execution(
        subblock_client,
        args.block_number,
        agg_client,
        client_input,
        args.execute,
        args.prove,
        args.dump_dir,
    )
    .await?;

    println!("TIMER_ALL t_post_client_input: {:.3?}", t_post_client_input.elapsed());

    println!("TIMER_ALL entire main fn: {:.3?}", t_start.elapsed());

    Ok(())
}

async fn schedule_subblock_execution(
    subblock_client: DefaultProverClient,
    block_number: u64,
    agg_client: DefaultProverClient,
    inputs: SubblockHostOutput,
    execute: bool,
    _prove: bool,
    dump_dir: Option<PathBuf>,
) -> eyre::Result<()> {
    let t_dump = Instant::now();
    let out_dir = resolve_dump_dir(dump_dir.as_ref(), block_number);
    std::fs::create_dir_all(&out_dir)?;
    tracing::info!("Dump directory: {}", out_dir.display());

    println!(
        "TIMER aggregator stdin & dump_dir in schedule_subblock_execution: {:.3?}",
        t_dump.elapsed()
    );

    let t = Instant::now();
    // let mut riscv_proofs = Vec::new();
    // let mut combine_proofs = Vec::new();
    let _subblock_vk = subblock_client.riscv_vk().clone();

    for i in 0..inputs.subblock_inputs.len() {
        println!("----------------------Subblock {}-----------------------", i);
        let input = &inputs.subblock_inputs[i];
        let parent_state = &inputs.subblock_parent_states[i];

        let mut stdin_builder = subblock_client.new_stdin_builder();
        stdin_builder.write(input);
        stdin_builder.write_slice(parent_state);
        let filename = format!("subblock_stdin_builder_{}.bin", i);
        let f = BufWriter::new(File::create(out_dir.join(&filename))?);
        bincode::serialize_into(f, &stdin_builder)?;

        // Save the elf/stdin pair to the dump directory.
        // if let Some(dump_dir) = dump_dir.as_ref() {
        //     let stdin_dir_path = dump_dir.join("subblock_stdins");
        //     std::fs::create_dir_all(&stdin_dir_path)?;
        //     let stdin_path = stdin_dir_path.join(format!("{}.bin", i));
        //     std::fs::write(stdin_path, bincode::serialize(&stdin_builder)?)?;
        // }

        // TODO: use prove flag
        // Generate proof
        // let start = Instant::now();
        // let (riscv_proof, combine_proof) =
        //     subblock_client.prove_combine(stdin_builder.clone()).expect("Failed to generate
        // proof"); let elapsed = start.elapsed().as_secs_f64();

        // tracing::info!("Subblock {}: prove duration: {:?}", i, elapsed,);

        // riscv_proofs.push(riscv_proof);
        // combine_proofs.push(combine_proof);

        if execute {
            let start = Instant::now();

            let (cycles, _pv_stream) = subblock_client.emulate(stdin_builder.clone());

            let elapsed = start.elapsed().as_secs_f64();
            let subblock_instruction_count = cycles;
            let hz = subblock_instruction_count as f64 / elapsed;
            let mhz = hz / 1_000_000.0;

            tracing::info!(
                "Subblock {}: {} instructions in {:.3} s → {:.3} MHz",
                i,
                subblock_instruction_count,
                elapsed,
                mhz
            );
        }
    }
    println!("----------------------Aggregator-----------------------");

    let t_agg_stdin = Instant::now();
    // let aggregation_stdin = to_aggregation_stdin(inputs.clone(), &subblock_client.riscv_vk());

    let mut stdin_builder = agg_client.new_stdin_builder();

    let subblock_host_output = inputs;
    assert_eq!(
        subblock_host_output.subblock_inputs.len(),
        subblock_host_output.subblock_outputs.len()
    );
    let mut public_values = Vec::new();
    for i in 0..subblock_host_output.subblock_inputs.len() {
        let mut current_public_values = Vec::new();
        let input = &subblock_host_output.subblock_inputs[i];
        bincode::serialize_into(&mut current_public_values, input).unwrap();
        bincode::serialize_into(
            &mut current_public_values,
            &subblock_host_output.subblock_outputs[i],
        )
        .unwrap();
        public_values.push(current_public_values);
    }

    tracing::info!(
        "Public values size in bytes: {}",
        public_values.iter().map(|v| v.len()).sum::<usize>()
    );

    // // Deserialize the parent state and compute the root.
    // let mut aligned_vec = AlignedVec::<16>::new();
    // let mut reader = Cursor::new(&subblock_host_output.agg_parent_state);
    // aligned_vec.extend_from_reader(&mut reader).unwrap();
    // let parent_state =
    //     rkyv::from_bytes::<EthereumState, rkyv::rancor::BoxedError>(&aligned_vec).unwrap();
    // let parent_state_root = parent_state.state_root();

    // validate to execute aggregation
    {
        rsp_client_executor::ClientExecutor
            .execute_aggregation::<EthereumVariant>(
                public_values.clone(),
                subblock_client.riscv_vk().hash_u32(),
                subblock_host_output.agg_input.clone(),
                subblock_host_output.agg_input.parent_header().state_root,
            )
            .expect("failed to execute aggregation for validation");
    }

    let _ = dump_agg_stdin_to_files(
        &public_values,
        &subblock_client.riscv_vk().hash_u32(),
        &subblock_host_output.agg_input,
        &out_dir,
    );
    stdin_builder.write::<Vec<Vec<u8>>>(&public_values);
    stdin_builder.write::<[u32; 8]>(&subblock_client.riscv_vk().hash_u32());
    stdin_builder.write(&subblock_host_output.agg_input);
    stdin_builder.write(&subblock_host_output.agg_input.parent_header().state_root);

    let f = BufWriter::new(File::create(out_dir.join("aggregator_stdin_builder.bin"))?);
    bincode::serialize_into(f, &stdin_builder)?;
    // assert_eq!(riscv_proofs.len(), combine_proofs.len());
    // for i in 0..riscv_proofs.len() {
    //     stdin_builder.write_pico_proof(combine_proofs[i].clone(), subblock_vk.clone());
    // }

    let f = BufWriter::new(File::create(out_dir.join("final_aggregator_stdin_builder.bin"))?);
    bincode::serialize_into(f, &stdin_builder)?;

    println!("TIMER aggregator stdin: {:?}", t_agg_stdin.elapsed());

    // let start = Instant::now();
    // Execute the aggregation program with deferred proof verification off, since we don't have the
    // proof yet.
    // let (_agg_riscv_proof, _agg_combine_proof) =
    //     agg_client.prove_combine(stdin_builder.clone()).expect("Failed to generate proof");
    // let elapsed = start.elapsed().as_secs_f64();
    //
    // tracing::info!("Aggregator: prove duration: {:?}", elapsed,);

    if execute {
        let start = Instant::now();
        // Execute the aggregation program with deferred proof verification off, since we don't have
        // the proof yet.
        let (cycles, _pv_stream) = agg_client.emulate(stdin_builder);
        let elapsed = start.elapsed().as_secs_f64();

        let agg_instruction_count = cycles;
        let hz = agg_instruction_count as f64 / elapsed;
        let mhz = hz / 1_000_000.0;

        tracing::info!(
            "Aggregator: {} cycles/instructions in {:.3} s → {:.3} MHz",
            agg_instruction_count,
            elapsed,
            mhz
        );
        // tracing::info!("Aggregation program instruction count: {}", agg_instruction_count);
    }
    println!("TIMER execute subblocks and aggregator: {:?}", t.elapsed());

    Ok(())
}
//
// /// Constructs the aggregation stdin, minus the subblock proofs.
// pub fn to_aggregation_stdin(
//     subblock_host_output: SubblockHostOutput,
//     subblock_client: &DefaultProverClient,
//     agg_client: &DefaultProverClient,
// ) -> EmulatorStdinBuilder<Vec<u8>> {
//     let mut stdin_builder = agg_client.new_stdin_builder();
//
//     assert_eq!(
//         subblock_host_output.subblock_inputs.len(),
//         subblock_host_output.subblock_outputs.len()
//     );
//     let mut public_values = Vec::new();
//     for i in 0..subblock_host_output.subblock_inputs.len() {
//         let mut current_public_values = Vec::new();
//         let input = &subblock_host_output.subblock_inputs[i];
//         bincode::serialize_into(&mut current_public_values, input).unwrap();
//         bincode::serialize_into(
//             &mut current_public_values,
//             &subblock_host_output.subblock_outputs[i],
//         )
//         .unwrap();
//         public_values.push(current_public_values);
//     }
//
//     tracing::info!(
//         "Public values size in bytes: {}",
//         public_values.iter().map(|v| v.len()).sum::<usize>()
//     );
//
//     // // Deserialize the parent state and compute the root.
//     // let mut aligned_vec = AlignedVec::<16>::new();
//     // let mut reader = Cursor::new(&subblock_host_output.agg_parent_state);
//     // aligned_vec.extend_from_reader(&mut reader).unwrap();
//     // let parent_state =
//     //     rkyv::from_bytes::<EthereumState, rkyv::rancor::BoxedError>(&aligned_vec).unwrap();
//     // let parent_state_root = parent_state.state_root();
//
//     stdin_builder.write::<Vec<Vec<u8>>>(&public_values);
//     stdin_builder.write::<[u32; 8]>(&subblock_client.riscv_vk().hash_u32());
//     stdin_builder.write(&subblock_host_output.agg_input);
//     stdin_builder.write(&subblock_host_output.agg_input.parent_header().state_root);
//     stdin_builder
// }

fn dump_agg_stdin_to_files(
    public_values: &Vec<Vec<u8>>,
    vk_digest: &[u32; 8],
    agg_input: &AggregationInput,
    out_dir: &Path,
) -> std::io::Result<()> {
    // ensure directory exists
    std::fs::create_dir_all(out_dir)?;

    // 1. public_values
    let bytes = bincode::serialize(public_values).unwrap();
    File::create(out_dir.join("public_values.bin"))?.write_all(&bytes)?;

    // 2. vk_digest
    let bytes = bincode::serialize(vk_digest).unwrap();
    File::create(out_dir.join("vk_digest.bin"))?.write_all(&bytes)?;

    // 3. agg_input
    let bytes = bincode::serialize(agg_input).unwrap();
    File::create(out_dir.join("agg_input.bin"))?.write_all(&bytes)?;

    // 4. state_root
    let bytes = bincode::serialize(&agg_input.parent_header().state_root).unwrap();
    File::create(out_dir.join("state_root.bin"))?.write_all(&bytes)?;

    Ok(())
}

fn try_load_input_from_cache(
    cache_dir: Option<&PathBuf>,
    chain_id: u64,
    block_number: u64,
) -> eyre::Result<Option<SubblockHostOutput>> {
    Ok(if let Some(cache_dir) = cache_dir {
        let cache_path = cache_dir.join(format!("input/{}/{}.bin", chain_id, block_number));

        if cache_path.exists() {
            // Try to open and deserialize the cache file, delete it if there's an error
            match (|| -> eyre::Result<SubblockHostOutput> {
                let mut cache_file = std::fs::File::open(&cache_path)?;
                let cache_data: SubblockHostOutput = bincode::deserialize_from(&mut cache_file)?;
                Ok(cache_data)
            })() {
                Ok(cache_data) => Some(cache_data),
                Err(err) => {
                    tracing::warn!("Failed to load cache file {}: {}", cache_path.display(), err);
                    // Delete the invalid cache file
                    if let Err(delete_err) = std::fs::remove_file(&cache_path) {
                        tracing::warn!("Failed to delete invalid cache file: {}", delete_err);
                    } else {
                        tracing::info!("Deleted invalid cache file: {}", cache_path.display());
                    }
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

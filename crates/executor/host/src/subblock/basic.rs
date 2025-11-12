use super::{merge_state_requests, rpc_db_cache_dir_path};
use crate::{error::HostError, HostExecutor};
use alloy_consensus::{TxEnvelope, TxReceipt};
use alloy_network::Ethereum;
use alloy_primitives::Bloom;
use alloy_provider::Provider;
use reth_execution_types::ExecutionOutcome;
use reth_primitives_traits::{proofs, Block as BlockTrait};
use reth_trie::KeccakKeyHasher;
use revm::database::CacheDB;
use revm_primitives::{keccak256, B256};
use rsp_client_executor::{
    io::{AggregationInput, SubblockHostOutput, SubblockInput, SubblockOutput},
    EthereumVariant, Variant,
};
use rsp_mpt::EthereumState;
use rsp_rpc_db::{basic::BasicRpcDb, db::RpcDbTrait};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
    path::PathBuf,
    time::Instant,
};
use tokio::task::JoinSet;
use tracing::info;

/// cache filename of basic Rpc Db
const BASIC_RPC_CACHE_FILENAME: &str = "basic.db";

impl<P: Provider<Ethereum> + Clone + Debug + 'static> HostExecutor<P> {
    pub(crate) async fn execute_subblock_by_basic_rpc(
        &self,
        block_number: u64,
        dump_dir: Option<PathBuf>,
    ) -> Result<SubblockHostOutput, HostError> {
        // benchmark the whole running time
        let total_start = Instant::now();

        // fetch the current and the previous block
        let start = Instant::now();
        let current_block = self
            .basic_provider
            .get_block_by_number(block_number.into())
            .full()
            .await?
            .ok_or(HostError::ExpectedBlock(block_number))
            .map(|block| {
                let block = block.map_transactions(|tx| TxEnvelope::from(tx).into());
                block.into_consensus()
            })?;

        let previous_block = self
            .basic_provider
            .get_block_by_number((block_number - 1).into())
            .full()
            .await?
            .ok_or(HostError::ExpectedBlock(block_number))
            .map(|block| {
                let block = block.map_transactions(TxEnvelope::from);
                block.into_consensus()
            })?;
        info!("[bench] fetch the current and parent block: {:.3?}", start.elapsed());

        // set up rpc db
        let start = Instant::now();
        // build the basic Rpc Db cache file path
        let rpc_db_cache_path = basic_rpc_db_cache_file_path(dump_dir, block_number);

        // setup basic Rpc Db for block executor
        let mut rpc_db =
            BasicRpcDb::new(self.basic_provider.clone(), block_number - 1, &rpc_db_cache_path);

        info!("[bench] setup up RPC DB:  {:.3?}", start.elapsed());

        // execute the block and fetch all the necessary data along the way
        let start = Instant::now();
        let executor_block_input = EthereumVariant::pre_process_block(&current_block)
            .try_into_recovered()
            .map_err(|_| HostError::FailedToRecoverSenders)?;

        // accumulate across multiple subblocks
        let mut cumulative_executor_outcomes = ExecutionOutcome::default();
        let mut cumulative_state_requests = HashMap::new();

        // store individual state requests, executor outcomes, and state diffs for each subblock
        let mut all_state_requests = vec![];
        let mut all_executor_outcomes = vec![];
        let mut state_diffs = vec![];

        // total number and complete number of transactions
        let total_num_txs = current_block.body.transactions.len() as u64;
        let mut complete_num_txs = 0;

        // amount of gas used so far
        let mut cumulative_gas_used = 0;

        // accumulated logs bloom, across subblocks
        let mut global_logs_bloom = Bloom::default();

        // store the inputs, outputs, and parent states for each subblock
        let mut subblock_index = 0;
        let mut subblock_inputs = vec![];
        let mut subblock_outputs = vec![];
        let mut subblock_parent_states = vec![];
        info!("[bench] initalize subblock inputs and outputs: {:.3?}", start.elapsed());

        // compute the exactly gas limit for each subblock
        let start = Instant::now();
        let subblock_gas_limits = self.compute_subblock_gas_limits(&current_block).await;
        info!("[bench] compute subblock gas limits: {:.3?}", start.elapsed());

        info!("executing subblock");
        loop {
            let start = Instant::now();
            let subblock_gas_limit = subblock_gas_limits[subblock_index];
            info!(
                "subblock-{subblock_index}: total_num_transactions={total_num_txs}, complete_num_transactions={complete_num_txs}, subblock_gas_limit={subblock_gas_limit}",
            );
            subblock_index += 1;
            let cache_db = CacheDB::new(&rpc_db);

            // slice the block to only include the transactions that have not been executed yet
            let mut subblock_input = executor_block_input.clone();
            subblock_input.block.body.transactions =
                subblock_input.body.transactions[complete_num_txs as usize..].to_vec();
            subblock_input.senders = subblock_input.senders[complete_num_txs as usize..].to_vec();

            // set the subblock configuration
            let is_first_subblock = complete_num_txs == 0;
            subblock_input.is_first_subblock = is_first_subblock;
            subblock_input.is_last_subblock = false;
            subblock_input.subblock_gas_limit = subblock_gas_limit + cumulative_gas_used;
            subblock_input.starting_gas_used = cumulative_gas_used;
            let starting_gas_used = cumulative_gas_used;

            info!(
                "[bench] subblock-{subblock_index} prepare subblock input: {:.3?}",
                start.elapsed(),
            );

            let start = Instant::now();
            let spec = EthereumVariant::spec();
            let subblock_output = EthereumVariant::execute(&subblock_input, &spec, cache_db)?;
            info!("[bench] subblock-{subblock_index} execute subblock: {:.3?}", start.elapsed());

            // post process this subblock
            let start = Instant::now();
            let num_executed_transactions = subblock_output.receipts.len();
            let upper = complete_num_txs + num_executed_transactions as u64;
            let is_last_subblock = upper == current_block.body.transactions.len() as u64;
            info!(
                "successfully executed subblock: complete_num_transactions={}, upper={}",
                complete_num_txs, upper,
            );

            // accumulate the logs bloom
            let mut logs_bloom = Bloom::default();
            subblock_output.receipts.iter().for_each(|r| {
                logs_bloom.accrue_bloom(&r.bloom());
            });
            global_logs_bloom.accrue_bloom(&logs_bloom);

            // update the difference from the bundle state
            // the next subblock will see the state changes from the current subblock
            rpc_db.update_state_diffs(&subblock_output.state);

            // update the cumulative gas used
            let receipts = subblock_output.receipts.clone();
            cumulative_gas_used +=
                receipts.last().map(|r| r.cumulative_gas_used - starting_gas_used).unwrap_or(0);

            // convert the output to an execution outcome
            let executor_outcome = ExecutionOutcome::new(
                subblock_output.state.clone(),
                vec![subblock_output.receipts.clone()],
                current_block.header.number,
                vec![subblock_output.requests.clone()],
            );
            all_executor_outcomes.push(executor_outcome.clone());

            // save the subblock's post state for debugging
            let target_post_state = executor_outcome.hash_state_slow::<KeccakKeyHasher>();
            state_diffs.push(target_post_state);

            // initialize the subblock output
            let subblock_output = SubblockOutput {
                receipts,
                logs_bloom,
                output_state_root: B256::default(),
                input_state_root: B256::default(),
                requests: subblock_output.requests.clone(),
            };
            subblock_outputs.push(subblock_output);

            // accumulate this subblock's execution outcome
            cumulative_executor_outcomes.extend(executor_outcome);

            // record the state requests for this subblock
            let subblock_state_requests = rpc_db.state_requests();

            // merge the state requests
            merge_state_requests(&mut cumulative_state_requests, &subblock_state_requests);
            all_state_requests.push(subblock_state_requests);

            let mut subblock_input = SubblockInput {
                current_block: EthereumVariant::pre_process_block(&current_block),
                block_hashes: BTreeMap::new(),
                bytecodes: rpc_db.bytecodes(),
                is_first_subblock,
                is_last_subblock,
                starting_gas_used,
            };

            // slice the correct transactions for this subblock
            subblock_input.current_block.body.transactions =
                subblock_input.current_block.body.transactions
                    [complete_num_txs as usize..upper as usize]
                    .to_vec();

            // advance subblock
            complete_num_txs = upper;
            rpc_db.advance_subblock();

            subblock_inputs.push(subblock_input);

            info!("[bench] post process subblock-{subblock_index}: {:.3?}", start.elapsed());

            if complete_num_txs >= total_num_txs {
                break;
            }
        }

        // compute and verify the state root
        let start = Instant::now();
        if let Some(file_path) = rpc_db_cache_path {
            // store the Rpc Db cache data to a file
            rpc_db.store_cache(&file_path)?;
        }

        let parent_state = {
            // Build parent state from modified keys and used keys from this subblock
            let mut before_storage_proofs = Vec::new();
            let mut after_storage_proofs = Vec::new();

            let entries: Vec<_> = cumulative_state_requests.into_iter().collect();
            for chunk in entries.chunks(10) {
                let mut before_handles = JoinSet::new();
                let mut after_handles = JoinSet::new();
                for (address, used_keys) in chunk {
                    let address = *address;
                    let modified_keys = cumulative_executor_outcomes
                        .state()
                        .state
                        .get(&address)
                        .map(|account| {
                            account
                                .storage
                                .keys()
                                .map(|key| B256::from(*key))
                                .collect::<BTreeSet<_>>()
                        })
                        .unwrap_or_default()
                        .into_iter()
                        .collect::<Vec<_>>();

                    let keys = used_keys
                        .iter()
                        .map(|key| B256::from(*key))
                        .chain(modified_keys.clone().into_iter())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();

                    let provider_clone = self.basic_provider.clone();

                    before_handles.spawn(async move {
                        Self::get_proof(provider_clone, address, keys, block_number - 1)
                            .await
                            .unwrap()
                    });

                    let provider_clone = self.basic_provider.clone();
                    after_handles.spawn(async move {
                        Self::get_proof(provider_clone, address, modified_keys, block_number)
                            .await
                            .unwrap()
                    });
                }
                before_storage_proofs.extend(before_handles.join_all().await);
                after_storage_proofs.extend(after_handles.join_all().await);
            }

            EthereumState::from_transition_proofs(
                previous_block.state_root,
                &before_storage_proofs.iter().map(|item| (item.address, item.clone())).collect(),
                &after_storage_proofs.iter().map(|item| (item.address, item.clone())).collect(),
            )?
        };

        let mut cumulative_state_diffs =
            cumulative_executor_outcomes.hash_state_slow::<KeccakKeyHasher>();
        // TRICKY:
        // reth may return empty accounts, they must be deleted in the hash state,
        // otherwise the output state root was wrong
        cumulative_state_diffs.accounts.retain(|_, v| v.map(|acc| !acc.is_empty()).unwrap_or(true));

        // update the parent state with the cumulative state diffs from all subblocks
        let mut mutated_state = parent_state.clone();
        mutated_state.update(&cumulative_state_diffs);

        // verify the state root
        let state_root = mutated_state.state_root();
        if state_root != current_block.state_root {
            return Err(HostError::StateRootMismatch(state_root, current_block.state_root));
        }
        info!("[bench] verify state root: {:.3?}", start.elapsed());

        // derive the block header
        let start = Instant::now();
        let mut header = current_block.header.clone();
        header.parent_hash = previous_block.hash_slow();
        header.ommers_hash = proofs::calculate_ommers_root(&current_block.body.ommers);
        header.state_root = current_block.state_root;
        header.transactions_root =
            proofs::calculate_transaction_root(&current_block.body.transactions);
        header.receipts_root = current_block.header.receipts_root;
        header.withdrawals_root = current_block
            .body
            .withdrawals
            .clone()
            .map(|w| proofs::calculate_withdrawals_root(w.into_inner().as_slice()));
        header.logs_bloom = global_logs_bloom;
        header.requests_hash = current_block.header.requests_hash;

        // assert the derived header is correct
        let constructed_header_hash = header.hash_slow();
        let target_hash = current_block.header.hash_slow();
        if constructed_header_hash != target_hash {
            return Err(HostError::HeaderMismatch(constructed_header_hash, target_hash));
        }
        info!("[bench] derive block header: {:.3?}", start.elapsed());

        // fetch the parent headers needed to constrain the BLOCKHASH opcode
        let start = Instant::now();
        let (ancestor_headers, block_hashes) = {
            let oldest_ancestor = *rpc_db.oldest_ancestor.borrow();
            let mut ancestor_headers = vec![];
            let mut block_hashes = BTreeMap::new();
            tracing::info!("fetching {} ancestor headers", block_number - oldest_ancestor);
            for height in (oldest_ancestor..=(block_number - 1)).rev() {
                let block = self
                    .basic_provider
                    .get_block_by_number(height.into())
                    .await?
                    .ok_or(HostError::ExpectedBlock(height))?;

                block_hashes.insert(height, block.header.hash);
                ancestor_headers.push(block.header.into());
            }

            (ancestor_headers, block_hashes)
        };
        let aggregation_input = AggregationInput {
            current_block: EthereumVariant::pre_process_block(&current_block),
            ancestor_headers,
        };
        info!("[bench] fetch ancestor headers: {:.3?}", start.elapsed());

        // compute each subblock input and output state root
        let start = Instant::now();
        let mut big_state = parent_state;
        for i in 0..subblock_inputs.len() {
            let input_root = big_state.state_root();
            // get the touched addresses and storage slots in this subblock
            let mut touched_state = HashMap::with_hasher(Default::default());
            for (address, used_keys) in all_state_requests[i].iter() {
                let modified_keys = all_executor_outcomes[i]
                    .state()
                    .state
                    .get(address)
                    .map(|account| {
                        account.storage.keys().map(|key| B256::from(*key)).collect::<BTreeSet<_>>()
                    })
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<Vec<_>>();

                let keys = used_keys
                    .iter()
                    .map(|key| B256::from(*key))
                    .chain(modified_keys.clone().into_iter())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(keccak256)
                    .collect::<Vec<_>>();

                touched_state.insert(keccak256(address), keys);
            }

            // generate the subblock parent state by taking the state diff of the subblock and all
            // touched addresses, and then pruning the big state
            let mut subblock_parent_state = big_state.clone();

            let serialized_size =
                rkyv::to_bytes::<rkyv::rancor::Error>(&subblock_parent_state).unwrap().len();
            let prev_root = subblock_parent_state.state_root();

            subblock_parent_state.prune(&state_diffs[i], &touched_state);

            // assert that pruning did not change the state root
            let new_root = subblock_parent_state.state_root();
            assert_eq!(prev_root, new_root);

            let new_serialized_size =
                rkyv::to_bytes::<rkyv::rancor::Error>(&subblock_parent_state).unwrap().len();
            tracing::info!(
                "Pruned state compression ratio: {}",
                new_serialized_size as f64 / serialized_size as f64
            );
            subblock_parent_states.push(
                rkyv::to_bytes::<rkyv::rancor::Error>(&subblock_parent_state).unwrap().to_vec(),
            );

            // TRICKY:
            // reth may return empty accounts, they must be deleted in the hash state,
            // otherwise the output state root was wrong
            state_diffs[i].accounts.retain(|_, v| v.map(|acc| !acc.is_empty()).unwrap_or(true));
            // update the big state with the state diff of this subblock, and set the fields of this
            // subblock's input and output accordingly
            big_state.update(&state_diffs[i]);
            let output_root = big_state.state_root();

            subblock_outputs[i].input_state_root = input_root;
            subblock_outputs[i].output_state_root = output_root;

            let subblock_input = &mut subblock_inputs[i];
            subblock_input.block_hashes = block_hashes.clone();
        }
        let all_subblock_outputs = SubblockHostOutput {
            subblock_inputs,
            subblock_parent_states,
            subblock_outputs,
            agg_input: aggregation_input,
        };
        // NOTE: this is useful for debugging, remove it in production.
        {
            // all_subblock_outputs.validate().expect("host and client outputs are different");
        }
        info!("[bench] compute each subblock state input and output: {:.3?}", start.elapsed());
        info!("[bench] execute_subblock_by_basic_rpc total time: {:.3?}", total_start.elapsed());

        Ok(all_subblock_outputs)
    }
}

// return the file path of basic Rpc Db cache
fn basic_rpc_db_cache_file_path(dump_dir: Option<PathBuf>, block_number: u64) -> Option<PathBuf> {
    rpc_db_cache_dir_path(dump_dir, block_number).map(|p| p.join(BASIC_RPC_CACHE_FILENAME))
}

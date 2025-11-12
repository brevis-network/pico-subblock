mod basic;
mod execution_witness;

use crate::{error::HostError, HostExecutor};
use alloy_consensus::BlockHeader;
use alloy_network::Ethereum;
use alloy_provider::Provider;
use revm_primitives::{Address, U256};
use rsp_client_executor::{io::SubblockHostOutput, ChainVariant};
use std::{collections::HashMap, fmt::Debug, path::PathBuf};
use tracing::info;

/// default subblock gas limit
const DEFAULT_SUBBLOCK_GAS_LIMIT: u64 = 1_000_000;

/// default Rpc data cache directory
const RPC_CACHE_DIR: &str = "rpc_db_cache";

lazy_static::lazy_static! {
    /// maximum subblock count to split
    pub static ref MAX_SUBBLOCK_COUNT: usize = std::env::var("MAX_SUBBLOCK_COUNT")
        .map(|s| s.parse().unwrap())
        .unwrap_or(7);
}

impl<P: Provider<Ethereum> + Clone + Debug + 'static> HostExecutor<P> {
    pub async fn execute_subblock(
        &self,
        use_execution_witness: bool,
        block_number: u64,
        variant: ChainVariant,
        dump_dir: Option<PathBuf>,
    ) -> Result<SubblockHostOutput, HostError> {
        info!("execute_subblock block_number={block_number}");
        match variant {
            ChainVariant::Ethereum => {
                if use_execution_witness {
                    self.execute_subblock_by_execution_witness_rpc(block_number, dump_dir).await
                } else {
                    self.execute_subblock_by_basic_rpc(block_number, dump_dir).await
                }
            }
        }
    }

    async fn compute_subblock_gas_limits(&self, block: &reth_primitives::Block) -> Vec<u64> {
        // call eth_getBlockReceipts to get gas used of each transaction
        let receipts =
            self.basic_provider.get_block_receipts(block.number.into()).await.unwrap().unwrap();
        assert_eq!(receipts.len(), block.body.transactions.len());

        // handle no transaction case
        if receipts.is_empty() {
            return vec![DEFAULT_SUBBLOCK_GAS_LIMIT];
        }

        // compute the minimum gas used for each subblock
        let total_gas = block.gas_used();
        let max_subblock_count = *MAX_SUBBLOCK_COUNT;
        let subblock_gas = (total_gas + max_subblock_count as u64) / max_subblock_count as u64;

        // previous collected gas, it's sum of subblock_gas_limits
        let mut prev = 0;
        // current accumulated gas
        let mut curr = 0;
        // next expected gas
        let mut next = subblock_gas;
        // subblock gas limit array for return
        let mut subblock_gas_limits = Vec::with_capacity(max_subblock_count);
        // iterate each transaction and compute the accumulated gas
        for receipt in receipts {
            // get the transaction gas
            let tx_gas = receipt.gas_used;

            // each subblock should contain one transaction at least
            // if plus the current transaction gas is greater than subblock_gas/2, consider this
            // transaction as a big transaction, add it to the next subblock
            if curr != prev && curr + tx_gas > next + (subblock_gas >> 1) {
                assert!(curr > prev);
                subblock_gas_limits.push(curr - prev);

                prev = curr;
                curr += tx_gas;
                next += subblock_gas;

                continue;
            }

            curr += tx_gas;
            if curr > next {
                subblock_gas_limits.push(curr - prev);

                prev = curr;
                next += subblock_gas;
            }
        }

        // add the last subblock
        if curr != prev {
            subblock_gas_limits.push(curr - prev);
        }

        // check subblock maximum count
        assert!(subblock_gas_limits.len() <= max_subblock_count);

        // check the total gas
        let all_subblock_gas: u64 = subblock_gas_limits.iter().sum();
        assert_eq!(total_gas, all_subblock_gas);

        println!(
            "block-{} is splitted to {} subblock gas limits: {:?}",
            block.number,
            subblock_gas_limits.len(),
            subblock_gas_limits,
        );

        subblock_gas_limits
    }
}

// merge state requuests
fn merge_state_requests(
    state_requests: &mut HashMap<Address, Vec<U256>>,
    subblock_state_requests: &alloy_primitives::map::HashMap<Address, Vec<U256>>,
) {
    for (address, keys) in subblock_state_requests.iter() {
        state_requests.entry(*address).or_default().extend(keys.iter().cloned());
    }
}

// return the directory path of Rpc Db cache
fn rpc_db_cache_dir_path(dump_dir: Option<PathBuf>, block_number: u64) -> Option<PathBuf> {
    dump_dir.map(|p| p.join(RPC_CACHE_DIR).join(format!("block_{block_number}")))
}

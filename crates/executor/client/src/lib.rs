#![allow(deprecated)]

/// Client program input data types.
pub mod io;
#[macro_use]
mod utils;
pub mod custom;
pub mod error;

use crate::custom::{CustomCrypto, CustomEvmFactory};
use alloy_consensus::TxReceipt;
use alloy_eips::eip7685::Requests;
use alloy_primitives::Bloom;
use cfg_if::cfg_if;
use error::ClientError;
use io::{AggregationInput, ClientExecutorInput, SubblockInput, SubblockOutput, TrieDB};
use itertools::Itertools;
use reth_chainspec::ChainSpec;
use reth_errors::ConsensusError;
use reth_ethereum_consensus::{
    validate_block_post_execution as validate_block_post_execution_ethereum,
    validate_subblock_post_execution as validate_subblock_post_execution_ethereum,
};
use reth_evm::{
    execute::{BasicBlockExecutor, BlockExecutionError, BlockExecutionOutput, Executor},
    Database,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_execution_types::ExecutionOutcome;
use reth_primitives::{Block, BlockWithSenders, Header, Receipt, TransactionSigned};
use reth_primitives_traits::{proofs, AlloyBlockHeader, Block as BlockTrait};
use reth_trie::KeccakKeyHasher;
use revm::{database::WrapDatabaseRef, install_crypto};
use revm_primitives::B256;
use rsp_mpt::EthereumState;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Cursor, iter::once};

/// Chain ID for Ethereum Mainnet.
pub const CHAIN_ID_ETH_MAINNET: u64 = 0x1;

/// Chain ID for OP Mainnnet.
pub const CHAIN_ID_OP_MAINNET: u64 = 0xa;

/// Chain ID for Linea Mainnet.
pub const CHAIN_ID_LINEA_MAINNET: u64 = 0xe708;

/// Chain ID for Sepolia.
pub const CHAIN_ID_SEPOLIA: u64 = 0xaa36a7;

/// An executor that executes a block inside a zkVM.
#[derive(Debug, Clone, Default)]
pub struct ClientExecutor;

/// Trait for representing different execution/validation rules of different chain variants. This
/// allows for dead code elimination to minimize the ELF size for each variant.
pub trait Variant {
    fn spec() -> ChainSpec;

    fn execute<DB: Database>(
        executor_block_input: &BlockWithSenders,
        chain_spec: &ChainSpec,
        cache_db: DB,
    ) -> Result<BlockExecutionOutput<Receipt>, BlockExecutionError>;

    fn validate_block_post_execution(
        block: &BlockWithSenders,
        chain_spec: &ChainSpec,
        receipts: &[Receipt],
        requests: &Requests,
    ) -> Result<(), ConsensusError>;

    fn validate_subblock_aggregation(
        _header: &Header,
        _chain_spec: &ChainSpec,
        _receipts: &[Receipt],
        _requests: &Requests,
    ) -> Result<(), ConsensusError> {
        unimplemented!()
    }

    fn pre_process_block(block: &Block) -> Block {
        block.clone()
    }
}

/// Implementation for Ethereum-specific execution/validation logic.
#[derive(Debug)]
pub struct EthereumVariant;

/// EVM chain variants that implement different execution/validation rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChainVariant {
    /// Ethereum networks.
    Ethereum,
}

impl ChainVariant {
    /// Returns the chain ID for the given variant.
    pub fn chain_id(&self) -> u64 {
        match self {
            ChainVariant::Ethereum => CHAIN_ID_ETH_MAINNET,
        }
    }
}

impl ClientExecutor {
    pub fn execute<V>(&self, mut input: ClientExecutorInput) -> Result<Header, ClientError>
    where
        V: Variant,
    {
        // Initialize the witnessed database with verified storage proofs.
        let wrap_ref = profile!("initialize witness db", {
            let trie_db = input.witness_db().unwrap();
            WrapDatabaseRef(trie_db)
        });

        // Execute the block.
        let spec = V::spec();
        let executor_block_input = profile!("recover senders", {
            input
                .current_block
                .clone()
                .try_into_recovered()
                .map_err(|_| ClientError::SignatureRecoveryFailed)
        })?;
        let executor_output =
            profile!("execute", { V::execute(&executor_block_input, &spec, wrap_ref) })?;

        // Validate the block post execution.
        profile!("validate block post-execution", {
            V::validate_block_post_execution(
                &executor_block_input,
                &spec,
                &executor_output.receipts,
                &executor_output.requests,
            )
        })?;

        // Accumulate the logs bloom.
        let mut logs_bloom = Bloom::default();
        profile!("accrue logs bloom", {
            executor_output.receipts.iter().for_each(|r| {
                logs_bloom.accrue_bloom(&r.bloom());
            })
        });

        // Convert the output to an execution outcome.
        let executor_outcome = ExecutionOutcome::new(
            executor_output.state,
            vec![executor_output.result.receipts],
            input.current_block.header.number,
            vec![executor_output.result.requests],
        );

        // Verify the state root.
        let state_root = profile!("compute state root", {
            let mut hash_state = executor_outcome.hash_state_slow::<KeccakKeyHasher>();
            // TRICKY: reth may return empty accounts, they must be deleted in the hash state,
            // otherwise the output state root was wrong.
            hash_state.accounts.retain(|_, v| v.map(|acc| !acc.is_empty()).unwrap_or(true));
            input.parent_state.update(&hash_state);
            input.parent_state.state_root()
        });

        if state_root != input.current_block.state_root {
            return Err(ClientError::MismatchedStateRoot);
        }

        // Derive the block header.
        //
        // Note: the receipts root and gas used are verified by `validate_block_post_execution`.
        let header = Header {
            parent_hash: input.current_block.header().parent_hash(),
            ommers_hash: input.current_block.header().ommers_hash(),
            beneficiary: input.current_block.header().beneficiary(),
            state_root,
            transactions_root: input.current_block.header().transactions_root(),
            receipts_root: input.current_block.header().receipts_root(),
            logs_bloom: input.current_block.logs_bloom,
            difficulty: input.current_block.header().difficulty(),
            number: input.current_block.header().number(),
            gas_limit: input.current_block.header().gas_limit(),
            gas_used: input.current_block.header().gas_used(),
            timestamp: input.current_block.header().timestamp(),
            extra_data: input.current_block.header().extra_data().clone(),
            mix_hash: input.current_block.header().mix_hash().unwrap(),
            nonce: input.current_block.header().nonce().unwrap(),
            base_fee_per_gas: input.current_block.header().base_fee_per_gas(),
            withdrawals_root: input.current_block.header().withdrawals_root(),
            blob_gas_used: input.current_block.header().blob_gas_used(),
            excess_blob_gas: input.current_block.header().excess_blob_gas(),
            parent_beacon_block_root: input.current_block.header().parent_beacon_block_root(),
            requests_hash: input.current_block.header().requests_hash(),
        };

        Ok(header)
    }

    /// Executes a SubblockInput, and returns a SubblockOutput.
    pub fn execute_subblock<V>(
        &self,
        input: SubblockInput,
        input_state: &mut EthereumState,
    ) -> Result<SubblockOutput, ClientError>
    where
        V: Variant,
    {
        let input_state_root = profile!("compute input state root", { input_state.state_root() });

        let wrap_ref = profile!("construct trie db", {
            // Finally, construct the database.
            let bytecode_by_hash = input.bytecodes.iter().map(|b| (b.hash_slow(), b)).collect();
            let trie_db = TrieDB::new(input_state, input.block_hashes, bytecode_by_hash);
            WrapDatabaseRef(trie_db)
        });

        // Execute the block.
        let spec = V::spec();
        let mut executor_block_input = profile!("recover senders", {
            input
                .current_block
                .clone()
                .try_into_recovered()
                .map_err(|_| ClientError::SignatureRecoveryFailed)
        })?;
        executor_block_input.is_first_subblock = input.is_first_subblock;
        executor_block_input.is_last_subblock = input.is_last_subblock;
        executor_block_input.starting_gas_used = input.starting_gas_used;
        let executor_output =
            profile!("execute", { V::execute(&executor_block_input, &spec, wrap_ref) })?;

        let requests = executor_output.requests.clone();
        let receipts = executor_output.receipts.clone();

        // Accumulate the logs bloom.
        let mut logs_bloom = Bloom::default();
        profile!("accrue logs bloom", {
            executor_output.receipts.iter().for_each(|r| {
                logs_bloom.accrue_bloom(&r.bloom());
            })
        });

        let subblock_output = profile!("finalize output", {
            // Convert the output to an execution outcome.
            let executor_outcome = ExecutionOutcome::new(
                executor_output.state,
                vec![executor_output.result.receipts],
                input.current_block.header.number,
                vec![executor_output.result.requests],
            );

            let mut hash_state = executor_outcome.hash_state_slow::<KeccakKeyHasher>();
            // TRICKY: reth may return empty accounts, they must be deleted in the hash state,
            // otherwise the output state root was wrong.
            hash_state.accounts.retain(|_, v| v.map(|acc| !acc.is_empty()).unwrap_or(true));

            // Get the output state root by applying the diff to the input state.
            input_state.update(&hash_state);
            let output_state_root = input_state.state_root();

            SubblockOutput { output_state_root, logs_bloom, receipts, input_state_root, requests }
        });

        Ok(subblock_output)
    }

    /// Executes the aggregation of multiple subblocks.
    ///
    /// When executed in the zkvm, this will verify all of the subblock proofs and perform
    /// consistency checks between them. The aggregation input is committed as a public value, so
    /// it is taken as a trusted input.
    #[allow(unused)]
    pub fn execute_aggregation<V: Variant>(
        &self,
        public_values: Vec<Vec<u8>>,
        vkey: [u32; 8],
        mut aggregation_input: AggregationInput,
        parent_state_root: B256,
    ) -> Result<Header, ClientError> {
        let mut cumulative_state_diff =
            SubblockOutput { output_state_root: parent_state_root, ..Default::default() };
        let mut transaction_body: Vec<TransactionSigned> = Vec::new();
        let mut block_hashes = None;
        profile!("aggregate", {
            for (i, public_value) in public_values.iter().enumerate() {
                let public_values_digest = Sha256::digest(public_value);
                cfg_if! {
                    if #[cfg(target_os = "zkvm")] {
                        pico_sdk::verify::verify_pico_proof(&vkey, &public_values_digest.into());
                    }
                }
                println!("cycle-tracker-start: deserialize subblock input");
                let mut reader = Cursor::new(&public_value);
                let subblock_input: SubblockInput = bincode::deserialize_from(&mut reader).unwrap();
                println!("cycle-tracker-end: deserialize subblock input");

                // Every subblock should have at least one block hash: the immediate parent block
                // hash. So an empty block_hashes indicates that this is the first
                // subblock.
                if i == 0 && block_hashes.is_none() {
                    block_hashes = Some(subblock_input.block_hashes);
                } else {
                    assert_eq!(block_hashes, Some(subblock_input.block_hashes));
                }

                // Check that the starting gas used is the same as the last cumulative gas used.
                assert_eq!(
                    subblock_input.starting_gas_used,
                    cumulative_state_diff
                        .receipts
                        .last()
                        .map(|r| r.cumulative_gas_used)
                        .unwrap_or(0)
                );

                // Consistency checks on the subblock input's first/last subblock flags.
                if i == 0 {
                    assert!(subblock_input.is_first_subblock);
                }
                if i == public_values.len() - 1 {
                    assert!(subblock_input.is_last_subblock);
                }
                if i > 0 && i < public_values.len() - 1 {
                    assert!(!subblock_input.is_first_subblock);
                    assert!(!subblock_input.is_last_subblock);
                }

                // Check that the subblock header, ommers, withdrawals, and requests are the same as
                // the main block.
                assert_eq!(
                    subblock_input.current_block.header,
                    aggregation_input.current_block.header
                );
                assert_eq!(
                    subblock_input.current_block.body.ommers,
                    aggregation_input.current_block.body.ommers
                );
                assert_eq!(
                    subblock_input.current_block.body.withdrawals,
                    aggregation_input.current_block.body.withdrawals
                );
                assert_eq!(
                    subblock_input.current_block.header.requests_hash,
                    aggregation_input.current_block.header.requests_hash
                );
                println!("cycle-tracker-start: deserialize subblock output");

                let subblock_output: SubblockOutput =
                    bincode::deserialize_from(&mut reader).unwrap();
                println!("cycle-tracker-end: deserialize subblock output");

                println!("cycle-tracker-start: extend state");

                // Accumulate subblock's output into the cumulative state diff.
                // This function also contains consistency checks between the cumulative state diff
                // and the subblock output.
                cumulative_state_diff.extend(subblock_output);

                // Also add this subblock's transaction body to the transaction body.
                transaction_body.extend(subblock_input.current_block.body.transactions);
                println!("cycle-tracker-end: extend state");
            }
        });

        // Merge the same type requests.
        cumulative_state_diff.merge_requests();

        profile!("verify block hashes", {
            let mut reconstructed_block_hashes: BTreeMap<u64, B256> = BTreeMap::new();
            for (child_header, parent_header) in once(&aggregation_input.current_block.header)
                .chain(aggregation_input.ancestor_headers.iter())
                .tuple_windows()
            {
                assert!(parent_header.number == child_header.number - 1);

                let parent_header_hash = parent_header.hash_slow();
                assert_eq!(parent_header_hash, child_header.parent_hash);

                reconstructed_block_hashes.insert(parent_header.number, parent_header_hash);
            }

            assert_eq!(reconstructed_block_hashes, block_hashes.unwrap());
        });

        // Check that the subblock transactions match the main block transactions.
        assert_eq!(
            transaction_body, aggregation_input.current_block.body.transactions,
            "subblock transactions do not match main block transactions"
        );

        profile!("validate subblock aggregation", {
            // Check that the accumulated logs bloom is the same as the main block logs bloom.
            assert_eq!(
                cumulative_state_diff.logs_bloom,
                aggregation_input.current_block.header.logs_bloom
            );
            V::validate_subblock_aggregation(
                &aggregation_input.current_block.header,
                &V::spec(),
                &cumulative_state_diff.receipts,
                &cumulative_state_diff.requests,
            )
            .expect("failed to validate subblock aggregation")
        });

        // The final state root of the entire block is the cumulative output state root.
        let state_root = cumulative_state_diff.output_state_root;
        if state_root != aggregation_input.current_block.state_root {
            panic!(
                "mismatched state root: {state_root} != {:?}",
                aggregation_input.current_block.state_root
            );
        }

        // Derive the block header.
        //
        // Note: the receipts root and gas used are verified by `validate_subblock_aggregation`.
        let mut header = aggregation_input.current_block.header.clone();
        header.parent_hash = aggregation_input.parent_header().hash_slow();
        header.ommers_hash =
            proofs::calculate_ommers_root(&aggregation_input.current_block.body.ommers);
        header.state_root = aggregation_input.current_block.state_root;
        header.transactions_root =
            proofs::calculate_transaction_root(&aggregation_input.current_block.body.transactions);
        header.receipts_root = aggregation_input.current_block.header.receipts_root;
        header.withdrawals_root = aggregation_input
            .current_block
            .body
            .withdrawals
            .take()
            .map(|w| proofs::calculate_withdrawals_root(w.into_inner().as_slice()));
        header.logs_bloom = cumulative_state_diff.logs_bloom;
        header.requests_hash = aggregation_input.current_block.header.requests_hash;

        Ok(header)
    }
}

impl Variant for EthereumVariant {
    fn spec() -> ChainSpec {
        rsp_primitives::chain_spec::mainnet()
    }

    fn execute<DB: Database>(
        executor_block_input: &BlockWithSenders,
        chain_spec: &ChainSpec,
        cache_db: DB,
    ) -> Result<BlockExecutionOutput<Receipt>, BlockExecutionError> {
        install_crypto(CustomCrypto::default());

        let evm_config =
            EthEvmConfig::new_with_evm_factory(chain_spec.clone().into(), CustomEvmFactory);
        BasicBlockExecutor::new(evm_config, cache_db).execute(executor_block_input)
    }

    fn validate_block_post_execution(
        block: &BlockWithSenders,
        chain_spec: &ChainSpec,
        receipts: &[Receipt],
        requests: &Requests,
    ) -> Result<(), ConsensusError> {
        validate_block_post_execution_ethereum(block, chain_spec, receipts, requests)
    }

    fn validate_subblock_aggregation(
        header: &Header,
        chain_spec: &ChainSpec,
        receipts: &[Receipt],
        requests: &Requests,
    ) -> Result<(), ConsensusError> {
        validate_subblock_post_execution_ethereum(header, chain_spec, receipts, requests)
    }
}

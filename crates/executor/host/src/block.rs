use crate::{error::HostError, HostExecutor};
use alloy_consensus::{Block, TxEnvelope, TxReceipt};
use alloy_network::Ethereum;
use alloy_primitives::Bloom;
use alloy_provider::Provider;
use reth_execution_types::ExecutionOutcome;
use reth_primitives_traits::{proofs, Block as BlockTrait};
use reth_trie::KeccakKeyHasher;
use revm::database::CacheDB;
use revm_primitives::B256;
use rsp_client_executor::{io::ClientExecutorInput, ChainVariant, EthereumVariant, Variant};
use rsp_mpt::EthereumState;
use rsp_primitives::account_proof::eip1186_proof_to_account_proof;
use rsp_rpc_db::{basic::BasicRpcDb, db::RpcDbTrait};
use std::{collections::BTreeSet, fmt::Debug};

impl<P: Provider<Ethereum> + Clone + Debug + 'static> HostExecutor<P> {
    /// Executes the block with the given block number.
    pub async fn execute_block(
        &self,
        block_number: u64,
        variant: ChainVariant,
    ) -> Result<ClientExecutorInput, HostError> {
        tracing::info!("execute block_number={block_number}");
        match variant {
            ChainVariant::Ethereum => self.execute_variant(block_number).await,
        }
    }

    async fn execute_variant(&self, block_number: u64) -> Result<ClientExecutorInput, HostError> {
        // Fetch the current block and the previous block from the provider.
        tracing::info!("fetching the current block and the previous block");

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

        let previous_block: Block<_> = self
            .basic_provider
            .get_block_by_number((block_number - 1).into())
            .full()
            .await?
            .ok_or(HostError::ExpectedBlock(block_number))
            .map(|block| {
                let block = block.map_transactions(TxEnvelope::from);
                block.into_consensus()
            })?;

        // Setup the spec for the block executor.
        tracing::info!("setting up the spec for the block executor");

        // Setup the database for the block executor.
        tracing::info!("setting up the database for the block executor");
        let rpc_db = BasicRpcDb::new(self.basic_provider.clone(), block_number - 1, &None);
        let cache_db = CacheDB::new(&rpc_db);

        // Execute the block and fetch all the necessary data along the way.
        tracing::info!(
            "executing the block and with rpc db: block_number={}, transaction_count={}",
            block_number,
            current_block.body.transactions.len()
        );

        let executor_block_input = EthereumVariant::pre_process_block(&current_block)
            .try_into_recovered()
            .map_err(|_| HostError::FailedToRecoverSenders)?;

        let spec = EthereumVariant::spec();
        let executor_output = EthereumVariant::execute(&executor_block_input, &spec, cache_db)?;

        // Validate the block post execution.
        tracing::info!("validating the block post execution");
        EthereumVariant::validate_block_post_execution(
            &executor_block_input,
            &spec,
            &executor_output.receipts,
            &executor_output.requests,
        )?;

        // Accumulate the logs bloom.
        tracing::info!("accumulating the logs bloom");
        let mut logs_bloom = Bloom::default();
        executor_output.receipts.iter().for_each(|r| {
            logs_bloom.accrue_bloom(&r.bloom());
        });

        // Convert the output to an execution outcome.
        let executor_outcome = ExecutionOutcome::new(
            executor_output.state,
            vec![executor_output.result.receipts],
            current_block.header.number,
            vec![executor_output.result.requests],
        );

        let state_requests = rpc_db.state_requests();

        // For every account we touched, fetch the storage proofs for all the slots we touched.
        tracing::info!("fetching storage proofs");
        let mut before_storage_proofs = Vec::new();
        let mut after_storage_proofs = Vec::new();

        for (address, used_keys) in state_requests.iter() {
            let modified_keys = executor_outcome
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
                .collect::<Vec<_>>();

            let storage_proof = self
                .basic_provider
                .get_proof(*address, keys.clone())
                .block_id((block_number - 1).into())
                .await?;
            before_storage_proofs.push(eip1186_proof_to_account_proof(storage_proof));

            let storage_proof = self
                .basic_provider
                .get_proof(*address, modified_keys)
                .block_id((block_number).into())
                .await?;
            after_storage_proofs.push(eip1186_proof_to_account_proof(storage_proof));
        }

        let state = EthereumState::from_transition_proofs(
            previous_block.state_root,
            &before_storage_proofs.iter().map(|item| (item.address, item.clone())).collect(),
            &after_storage_proofs.iter().map(|item| (item.address, item.clone())).collect(),
        )?;

        // Verify the state root.
        tracing::info!("verifying the state root");
        let state_root = {
            let mut mutated_state = state.clone();
            let mut hash_state = executor_outcome.hash_state_slow::<KeccakKeyHasher>();
            // TRICKY: reth may return empty accounts, they must be deleted in the hash state,
            // otherwise the output state root was wrong.
            hash_state.accounts.retain(|_, v| v.map(|acc| !acc.is_empty()).unwrap_or(true));
            mutated_state.update(&hash_state);
            mutated_state.state_root()
        };
        if state_root != current_block.state_root {
            return Err(HostError::StateRootMismatch(state_root, current_block.state_root));
        }

        // Derive the block header.
        //
        // Note: the receipts root and gas used are verified by `validate_block_post_execution`.
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
        header.logs_bloom = logs_bloom;
        header.requests_hash = current_block.header.requests_hash;

        // Assert the derived header is correct.
        let constructed_header_hash = header.hash_slow();
        let target_hash = current_block.header.hash_slow();
        if constructed_header_hash != target_hash {
            return Err(HostError::HeaderMismatch(constructed_header_hash, target_hash));
        }

        // Log the result.
        tracing::info!(
            "successfully executed block: block_number={}, block_hash={}, state_root={}",
            current_block.header.number,
            header.hash_slow(),
            state_root
        );

        // Fetch the parent headers needed to constrain the BLOCKHASH opcode.
        let oldest_ancestor = *rpc_db.oldest_ancestor.borrow();
        let mut ancestor_headers = vec![];
        tracing::info!("fetching {} ancestor headers", block_number - oldest_ancestor);
        for height in (oldest_ancestor..=(block_number - 1)).rev() {
            let block = self
                .basic_provider
                .get_block_by_number(height.into())
                .await?
                .ok_or(HostError::ExpectedBlock(height))?;

            ancestor_headers.push(block.header.into());
        }

        // Create the client input.
        let client_input = ClientExecutorInput {
            current_block: EthereumVariant::pre_process_block(&current_block),
            ancestor_headers,
            parent_state: state,
            state_requests,
            bytecodes: rpc_db.bytecodes(),
        };
        tracing::info!("successfully generated client input");

        Ok(client_input)
    }
}

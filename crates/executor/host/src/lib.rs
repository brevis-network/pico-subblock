mod block;
mod error;
mod subblock;

use alloy_network::Ethereum;
use alloy_provider::Provider;
use error::HostError;
use reth_trie::AccountProof;
use revm_primitives::{Address, B256};
use rsp_primitives::account_proof::eip1186_proof_to_account_proof;
use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::time::sleep;

/// maximum number of times to retry fetching a proof
const MAX_PROOF_RETRIES: u32 = 5;

/// initial backoff duration for proof fetching retries
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(1000);

/// An executor that fetches data from a [Provider] to execute blocks in the [ClientExecutor].
#[derive(Debug, Clone)]
pub struct HostExecutor<P: Provider<Ethereum> + Clone> {
    pub basic_provider: Arc<P>,
    pub debug_provider: Arc<P>,
}

impl<P: Provider<Ethereum> + Clone + Debug + 'static> HostExecutor<P> {
    /// Create a new [`HostExecutor`] with a specific [Provider] and [Transport].
    pub fn new(basic_provider: P, debug_provider: P) -> Self {
        Self { basic_provider: Arc::new(basic_provider), debug_provider: Arc::new(debug_provider) }
    }

    async fn get_proof(
        provider: Arc<P>,
        address: Address,
        keys: Vec<B256>,
        block_number: u64,
    ) -> Result<AccountProof, HostError> {
        let mut attempts = 0;
        let mut backoff = INITIAL_RETRY_BACKOFF;

        loop {
            match provider.get_proof(address, keys.clone()).block_id((block_number).into()).await {
                Ok(proof) => return Ok(eip1186_proof_to_account_proof(proof)),
                Err(e) => {
                    attempts += 1;
                    if attempts >= MAX_PROOF_RETRIES {
                        tracing::error!(
                            "Failed to get proof for address {} at block {} after {} attempts: {:?}",
                            address,
                            block_number,
                            attempts,
                            e
                        );
                        // Consider returning a more specific error if needed
                        return Err(HostError::Transport(e));
                    }
                    tracing::warn!(
                        "Attempt {} failed to get proof for address {} at block {}. Retrying in {:?}...",
                        attempts,
                        address,
                        block_number,
                        backoff
                    );
                    sleep(backoff).await;
                    // Exponential backoff
                    backoff *= 2;
                }
            }
        }
    }
}

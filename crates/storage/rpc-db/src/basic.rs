use crate::{
    db::RpcDbTrait,
    error::RpcDbError,
    state::RpcDbState,
    utils::{load_serde_data, store_serde_data},
};
use alloy_network_primitives::HeaderResponse;
use alloy_provider::{network::BlockResponse, Network, Provider};
use alloy_rpc_types::BlockId;
use reth_storage_errors::{db::DatabaseError, provider::ProviderError};
use revm_database::BundleState;
use revm_database_interface::DatabaseRef;
use revm_primitives::{Address, HashMap, B256, U256};
use revm_state::{AccountInfo, Bytecode};
use std::{
    cell::RefCell,
    marker::PhantomData,
    path::{Path, PathBuf},
};

/// A database that fetches data from a [Provider] over a [Transport].
#[derive(Clone, Debug)]
pub struct BasicRpcDb<P, N> {
    /// The provider which fetches data.
    pub provider: P,
    /// The block to fetch data from.
    pub block: BlockId,
    /// The block hashes.
    pub block_hashes: RefCell<HashMap<u64, B256>>,
    /// The subblock data, used for each subblock.
    pub subblock_data: RefCell<RpcDbState>,
    /// The persistent data, used across multiple subblocks.
    pub persistent_data: RefCell<RpcDbState>,
    /// The cache data, used for replay.
    pub cache_data: RefCell<RpcDbState>,
    /// The oldest block whose header/hash has been requested.
    pub oldest_ancestor: RefCell<u64>,
    /// A phantom type to make the struct generic over the transport.
    pub _phantom: PhantomData<N>,
}

impl<P: Provider<N> + Clone, N: Network> BasicRpcDb<P, N> {
    /// Create a new [`BasicRpcDb`].
    pub fn new(provider: P, block: u64, cache_file_path: &Option<PathBuf>) -> Self {
        let cache_data =
            cache_file_path.as_ref().and_then(|p| load_serde_data(p)).unwrap_or_default();

        BasicRpcDb {
            provider,
            block: block.into(),
            block_hashes: RefCell::new(HashMap::with_hasher(Default::default())),
            subblock_data: RefCell::new(Default::default()),
            persistent_data: RefCell::new(Default::default()),
            cache_data: RefCell::new(cache_data),
            oldest_ancestor: RefCell::new(block),
            _phantom: PhantomData,
        }
    }

    /// Fetch the [AccountInfo] for an [Address].
    pub async fn fetch_account_info(&self, address: Address) -> Result<AccountInfo, RpcDbError> {
        tracing::debug!("fetching account info for address: {}", address);

        // Prioritize fetching from the persistent or cache data.
        if self.persistent_data.borrow().accounts.contains_key(&address) {
            let account_info =
                self.persistent_data.borrow().accounts.get(&address).unwrap().clone();
            self.subblock_data.borrow_mut().accounts.insert(address, account_info.clone());

            return Ok(account_info);
        }
        if self.cache_data.borrow().accounts.contains_key(&address) {
            let account_info = self.cache_data.borrow().accounts.get(&address).unwrap().clone();
            self.subblock_data.borrow_mut().accounts.insert(address, account_info.clone());

            return Ok(account_info);
        }

        // Fetch the proof for the account.
        let proof = self
            .provider
            .get_proof(address, vec![])
            .block_id(self.block)
            .await
            .map_err(|e| RpcDbError::GetProofError(address, e.to_string()))?;

        // Fetch the code of the account.
        let code = self
            .provider
            .get_code_at(address)
            .block_id(self.block)
            .await
            .map_err(|e| RpcDbError::GetCodeError(address, e.to_string()))?;

        // Construct the account info & write it to the log.
        let bytecode = Bytecode::new_raw(code);
        let account_info = AccountInfo {
            nonce: proof.nonce,
            balance: proof.balance,
            code_hash: proof.code_hash,
            code: Some(bytecode.clone()),
        };

        // Record the account info to the state.
        self.subblock_data.borrow_mut().accounts.insert(address, account_info.clone());
        self.cache_data.borrow_mut().accounts.insert(address, account_info.clone());

        Ok(account_info)
    }

    /// Fetch the storage value at an [Address] and [U256] index.
    pub async fn fetch_storage_at(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, RpcDbError> {
        tracing::debug!("fetching storage value at address: {}, index: {}", address, index);

        // Prioritize fetching from the persistent or cache data.
        if let Some(storage_map) = self.persistent_data.borrow().storage.get(&address) {
            if let Some(value) = storage_map.get(&index) {
                // Record the storage value to the subblock state.
                let mut storage_values = self.subblock_data.borrow_mut();
                let entry = storage_values.storage.entry(address).or_default();
                entry.insert(index, *value);
                return Ok(*value);
            }
        }
        if let Some(storage_map) = self.cache_data.borrow().storage.get(&address) {
            if let Some(value) = storage_map.get(&index) {
                // Record the storage value to the subblock state.
                let mut storage_values = self.subblock_data.borrow_mut();
                let entry = storage_values.storage.entry(address).or_default();
                entry.insert(index, *value);
                return Ok(*value);
            }
        }

        // Fetch the storage value.
        let value = self
            .provider
            .get_storage_at(address, index)
            .block_id(self.block)
            .await
            .map_err(|e| RpcDbError::GetStorageError(address, index, e.to_string()))?;

        // Record the storage value to the state.
        let mut storage_values = self.subblock_data.borrow_mut();
        let entry = storage_values.storage.entry(address).or_default();
        entry.insert(index, value);
        let mut storage_values = self.cache_data.borrow_mut();
        let entry = storage_values.storage.entry(address).or_default();
        entry.insert(index, value);

        Ok(value)
    }

    /// Fetch the block hash for a block number.
    pub async fn fetch_block_hash(&self, number: u64) -> Result<B256, RpcDbError> {
        tracing::info!("fetching block hash for block number: {}", number);

        // Fetch the block.
        let block = self
            .provider
            .get_block_by_number(number.into())
            .await
            .map_err(|e| RpcDbError::GetBlockError(number, e.to_string()))?;

        // Record the block hash to the state.
        let block = block.ok_or(RpcDbError::BlockNotFound(number))?;
        let hash = block.header().hash();

        let mut oldest_ancestor = self.oldest_ancestor.borrow_mut();
        *oldest_ancestor = number.min(*oldest_ancestor);

        // Record the block hash to the state.
        self.block_hashes.borrow_mut().insert(number, hash);

        Ok(hash)
    }
}

impl<P: Provider<N> + Clone, N: Network> DatabaseRef for BasicRpcDb<P, N> {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ProviderError::Database(DatabaseError::Other("no tokio runtime found".to_string()))
        })?;
        let result =
            tokio::task::block_in_place(|| handle.block_on(self.fetch_account_info(address)));
        let account_info =
            result.map_err(|e| ProviderError::Database(DatabaseError::Other(e.to_string())))?;
        Ok(Some(account_info))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        unimplemented!()
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ProviderError::Database(DatabaseError::Other("no tokio runtime found".to_string()))
        })?;
        let result =
            tokio::task::block_in_place(|| handle.block_on(self.fetch_storage_at(address, index)));
        let value =
            result.map_err(|e| ProviderError::Database(DatabaseError::Other(e.to_string())))?;
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ProviderError::Database(DatabaseError::Other("no tokio runtime found".to_string()))
        })?;
        let result = tokio::task::block_in_place(|| handle.block_on(self.fetch_block_hash(number)));
        let value =
            result.map_err(|e| ProviderError::Database(DatabaseError::Other(e.to_string())))?;
        Ok(value)
    }
}

impl<P: Provider<N> + Clone, N: Network> RpcDbTrait for BasicRpcDb<P, N> {
    fn bytecodes(&self) -> Vec<Bytecode> {
        let subblock_data = self.subblock_data.borrow();
        subblock_data.bytecodes()
    }

    fn state_requests(&self) -> HashMap<Address, Vec<U256>> {
        let subblock_data = self.subblock_data.borrow();
        subblock_data.state_requests()
    }

    fn advance_subblock(&self) {
        self.subblock_data.borrow_mut().clear();
    }

    fn update_state_diffs(&mut self, state_diffs: &BundleState) {
        for (address, account) in state_diffs.state.iter() {
            match &account.info {
                Some(info) => {
                    self.persistent_data.borrow_mut().accounts.insert(*address, info.clone())
                }
                None => {
                    // This indicates a destroyed account.
                    self.persistent_data
                        .borrow_mut()
                        .accounts
                        .insert(*address, AccountInfo::default())
                }
            };
            account.storage.iter().for_each(|(k, v)| {
                self.persistent_data
                    .borrow_mut()
                    .storage
                    .entry(*address)
                    .or_default()
                    .insert(*k, v.present_value());
            });
        }
    }

    fn store_cache(&self, file_path: &Path) -> Result<(), RpcDbError> {
        store_serde_data(file_path, &*self.cache_data.borrow())?;

        Ok(())
    }
}

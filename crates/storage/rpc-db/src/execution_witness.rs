use crate::{
    db::RpcDbTrait,
    error::RpcDbError,
    state::{ExecutionWitnessState, RpcDbState},
    utils::{load_serde_data, store_serde_data},
};
use alloy_consensus::Header;
use alloy_primitives::{map::HashMap, Address, B256};
use alloy_provider::{ext::DebugApi, Network, Provider};
use alloy_rlp::Decodable;
use alloy_rpc_types::BlockId;
use alloy_trie::TrieAccount;
use reth_storage_errors::ProviderError;
use revm_database::{BundleState, DatabaseRef};
use revm_primitives::{keccak256, ruint::aliases::U256, StorageKey, StorageValue};
use revm_state::{AccountInfo, Bytecode};
use rsp_mpt::EthereumState;
use std::{
    cell::RefCell,
    marker::PhantomData,
    path::{Path, PathBuf},
    time::Instant,
};
use tokio::{runtime::Handle, task::block_in_place};
use tracing::{info, warn};

#[derive(Debug)]
pub struct ExecutionWitnessRpcDb<P, N> {
    pub block_number: BlockId,
    pub basic_provider: P,
    pub full_state: ExecutionWitnessState,
    pub subblock_state: RefCell<RpcDbState>,
    pub persistent_state: RefCell<RpcDbState>,
    phantom: PhantomData<N>,
}

impl<P: Provider<N> + Clone, N: Network> ExecutionWitnessRpcDb<P, N> {
    /// Create a new [`ExecutionWitnessRpcDb`].
    pub async fn new(
        block_number: u64,
        state_root: B256,
        basic_provider: P,
        debug_provider: P,
        cache_file_path: &Option<PathBuf>,
    ) -> Result<Self, RpcDbError> {
        let execution_witness = if let Some(execution_witness) =
            cache_file_path.as_ref().and_then(|p| load_serde_data(p))
        {
            execution_witness
        } else {
            let start = Instant::now();
            let execution_witness =
                debug_provider.debug_execution_witness((block_number + 1).into()).await?;
            info!("debug_execution_witness RPC returns in {:?}", start.elapsed());
            if let Some(file_path) = cache_file_path {
                store_serde_data(file_path, &execution_witness)?;
            }

            execution_witness
        };

        let full_state = {
            let state = EthereumState::from_execution_witness(&execution_witness, state_root);

            let codes = execution_witness
                .codes
                .iter()
                .map(|encoded| (keccak256(encoded), Bytecode::new_raw(encoded.clone())))
                .collect();

            let ancestor_headers = execution_witness
                .headers
                .iter()
                .map(|encoded| Header::decode(&mut encoded.as_ref()).unwrap())
                .map(|h| (h.number, h))
                .collect();

            ExecutionWitnessState::new(state, codes, ancestor_headers)
        };

        let db = Self {
            block_number: block_number.into(),
            basic_provider,
            full_state,
            subblock_state: Default::default(),
            persistent_state: Default::default(),
            phantom: PhantomData,
        };

        Ok(db)
    }
}

impl<P: Provider<N> + Clone, N: Network> DatabaseRef for ExecutionWitnessRpcDb<P, N> {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if self.persistent_state.borrow().accounts.contains_key(&address) {
            let account_info =
                self.persistent_state.borrow().accounts.get(&address).unwrap().clone();
            self.subblock_state.borrow_mut().accounts.insert(address, account_info.clone());

            return Ok(Some(account_info));
        }

        let hash = keccak256(address);
        let account_info = if let Some(mut bytes) = self
            .full_state
            .state
            .state_trie
            .get(hash.as_ref())
            .map_err(|err| ProviderError::TrieWitnessError(err.to_string()))?
        {
            let account = TrieAccount::decode(&mut bytes)?;

            let code_hash = account.code_hash;
            let code = if let Ok(code) = self.code_by_hash_ref(code_hash) {
                code
            } else {
                // TODO: check the missing code hash
                warn!("missing code hash {code_hash} in debug_executionWitness response");
                let handle = Handle::try_current().unwrap();
                let bytes = block_in_place(|| {
                    handle.block_on(async {
                        self.basic_provider
                            .get_code_at(address)
                            .block_id(self.block_number)
                            .await
                            .unwrap()
                    })
                });
                Bytecode::new_raw(bytes)
            };

            AccountInfo {
                balance: account.balance,
                nonce: account.nonce,
                code_hash: account.code_hash,
                code: Some(code),
            }
        } else {
            AccountInfo::default().with_code_hash(B256::ZERO)
            /* only for debugging
                let handle = Handle::try_current().unwrap();
                block_in_place(|| {
                    handle.block_on(async {
                        let proof = self
                            .basic_provider
                            .get_proof(address, vec![])
                            .block_id(self.block_number)
                            .await
                            .map_err(|e| RpcDbError::GetProofError(address, e.to_string()))
                            .unwrap();

                        let code = self
                            .basic_provider
                            .get_code_at(address)
                            .block_id(self.block_number)
                            .await
                            .map_err(|e| RpcDbError::GetCodeError(address, e.to_string()))
                            .unwrap();

                        let bytecode = Bytecode::new_raw(code);
                        let account_info = AccountInfo {
                            nonce: proof.nonce,
                            balance: proof.balance,
                            code_hash: proof.code_hash,
                            code: Some(bytecode.clone()),
                        };
                        assert_eq!(account_info, AccountInfo::default().with_code_hash(B256::ZERO));
                        account_info
                    })
                })
            */
        };

        self.subblock_state.borrow_mut().accounts.insert(address, account_info.clone());

        Ok(Some(account_info))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.full_state
            .codes
            .get(&code_hash)
            .ok_or_else(|| {
                ProviderError::TrieWitnessError(format!("Code not found for {code_hash}"))
            })
            .cloned()
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        if let Some(storage_map) = self.persistent_state.borrow().storage.get(&address) {
            if let Some(value) = storage_map.get(&index) {
                let mut storage_values = self.subblock_state.borrow_mut();
                let entry = storage_values.storage.entry(address).or_default();
                entry.insert(index, *value);
                return Ok(*value);
            }
        }

        let slot = B256::from(index);
        let hashed_address = keccak256(address);
        let hashed_slot = keccak256(slot);
        let value = if let Some(mut value) = self
            .full_state
            .state
            .storage_tries
            .get(&hashed_address)
            .and_then(|storage_trie| storage_trie.get(hashed_slot.as_slice()).unwrap())
        {
            U256::decode(&mut value)?
        } else {
            U256::ZERO
            /* only for debugging
                let handle = Handle::try_current().unwrap();
                let value = block_in_place(|| {
                    handle.block_on(async {
                        self.basic_provider
                            .get_storage_at(address, index)
                            .block_id(self.block_number)
                            .await
                            .map_err(|e| RpcDbError::GetStorageError(address, index, e.to_string()))
                            .unwrap()
                    })
                });
                assert_eq!(value, U256::ZERO, "missing slot value must be zero");
                value
            */
        };

        let mut subblock_state = self.subblock_state.borrow_mut();
        let entry = subblock_state.storage.entry(address).or_default();
        entry.insert(index, value);

        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let header = self.full_state.ancestor_headers.get(&number).ok_or_else(|| {
            ProviderError::TrieWitnessError(format!("Header {number} not found in the ancestors"))
        })?;

        Ok(header.hash_slow())
    }
}

impl<P: Provider<N> + Clone, N: Network> RpcDbTrait for ExecutionWitnessRpcDb<P, N> {
    fn bytecodes(&self) -> Vec<Bytecode> {
        let subblock_state = self.subblock_state.borrow();
        subblock_state.bytecodes()
    }

    fn state_requests(&self) -> HashMap<Address, Vec<U256>> {
        let subblock_data = self.subblock_state.borrow();
        subblock_data.state_requests()
    }

    fn advance_subblock(&self) {
        self.subblock_state.borrow_mut().clear();
    }

    fn update_state_diffs(&mut self, state_diffs: &BundleState) {
        for (address, account) in state_diffs.state.iter() {
            match &account.info {
                Some(info) => {
                    self.persistent_state.borrow_mut().accounts.insert(*address, info.clone())
                }
                None => {
                    // This indicates a destroyed account.
                    self.persistent_state
                        .borrow_mut()
                        .accounts
                        .insert(*address, AccountInfo::default())
                }
            };
            account.storage.iter().for_each(|(k, v)| {
                self.persistent_state
                    .borrow_mut()
                    .storage
                    .entry(*address)
                    .or_default()
                    .insert(*k, v.present_value());
            });
        }
    }

    fn store_cache(&self, _file_path: &Path) -> Result<(), RpcDbError> {
        // we have already saved the execution witness Rpc data after request
        Ok(())
    }
}

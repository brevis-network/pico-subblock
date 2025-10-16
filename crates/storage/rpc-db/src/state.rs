use alloy_consensus::Header;
use alloy_primitives::B256;
use derive_more::Constructor;
use revm_primitives::{Address, HashMap, U256};
use revm_state::{AccountInfo, Bytecode};
use rsp_mpt::EthereumState;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcDbState {
    /// The persistent accounts, used across multiple subblocks.
    pub accounts: HashMap<Address, AccountInfo>,
    /// The persistent storage, used across multiple subblocks.
    pub storage: HashMap<Address, HashMap<U256, U256>>,
}

impl Default for RpcDbState {
    fn default() -> Self {
        Self {
            accounts: HashMap::with_hasher(Default::default()),
            storage: HashMap::with_hasher(Default::default()),
        }
    }
}

impl RpcDbState {
    // get the all account bytecodes
    pub fn bytecodes(&self) -> Vec<Bytecode> {
        let accounts = &self.accounts;

        accounts
            .values()
            .flat_map(|account| account.code.clone())
            .map(|code| (code.hash_slow(), code))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>()
    }

    // get the all the state keys which is used to read the actual state data from tries by client
    pub fn state_requests(&self) -> HashMap<Address, Vec<U256>> {
        let accounts = &self.accounts;
        let storage = &self.storage;

        accounts
            .keys()
            .chain(storage.keys())
            .map(|&address| {
                let storage_keys_for_address: BTreeSet<U256> = storage
                    .get(&address)
                    .map(|storage_map| storage_map.keys().cloned().collect())
                    .unwrap_or_default();

                (address, storage_keys_for_address.into_iter().collect())
            })
            .collect()
    }

    // clear the current state data
    pub fn clear(&mut self) {
        self.accounts.clear();
        self.storage.clear();
    }
}

#[derive(Clone, Debug, Constructor, Serialize, Deserialize)]
pub struct ExecutionWitnessState {
    pub state: EthereumState,
    pub codes: HashMap<B256, Bytecode>,
    pub ancestor_headers: HashMap<u64, Header>,
}

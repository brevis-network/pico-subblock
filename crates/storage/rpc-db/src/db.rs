use crate::{basic::BasicRpcDb, error::RpcDbError, execution_witness::ExecutionWitnessRpcDb};
use alloy_provider::{Network, Provider};
use reth_storage_errors::provider::ProviderError;
use revm_database::{BundleState, DatabaseRef};
use revm_primitives::{Address, HashMap, B256, U256};
use revm_state::{AccountInfo, Bytecode};
use std::path::Path;

pub trait RpcDbTrait: DatabaseRef {
    // get the all account bytecodes
    fn bytecodes(&self) -> Vec<Bytecode>;

    // get the all the state keys which is used to read the actual state data from tries by client
    fn state_requests(&self) -> HashMap<Address, Vec<U256>>;

    // reset the subblock state to get ready for the next subblock
    fn advance_subblock(&self);

    // accumulate the subblock state differences
    fn update_state_diffs(&mut self, state_diffs: &BundleState);

    // store Rpc Db cache data to a file
    fn store_cache(&self, file_path: &Path) -> Result<(), RpcDbError>;
}

#[derive(Debug)]
pub enum RpcDb<P, N> {
    Basic(BasicRpcDb<P, N>),
    ExecutionWitness(ExecutionWitnessRpcDb<P, N>),
}

impl<P: Provider<N> + Clone, N: Network> DatabaseRef for RpcDb<P, N> {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        match self {
            Self::Basic(rpc_db) => rpc_db.basic_ref(address),
            Self::ExecutionWitness(rpc_db) => rpc_db.basic_ref(address),
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        match self {
            Self::Basic(rpc_db) => rpc_db.code_by_hash_ref(code_hash),
            Self::ExecutionWitness(rpc_db) => rpc_db.code_by_hash_ref(code_hash),
        }
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        match self {
            Self::Basic(rpc_db) => rpc_db.storage_ref(address, index),
            Self::ExecutionWitness(rpc_db) => rpc_db.storage_ref(address, index),
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        match self {
            Self::Basic(rpc_db) => rpc_db.block_hash_ref(number),
            Self::ExecutionWitness(rpc_db) => rpc_db.block_hash_ref(number),
        }
    }
}

impl<P: Provider<N> + Clone, N: Network> RpcDbTrait for RpcDb<P, N> {
    fn bytecodes(&self) -> Vec<Bytecode> {
        match self {
            Self::Basic(rpc_db) => rpc_db.bytecodes(),
            Self::ExecutionWitness(rpc_db) => rpc_db.bytecodes(),
        }
    }

    fn state_requests(&self) -> HashMap<Address, Vec<U256>> {
        match self {
            Self::Basic(rpc_db) => rpc_db.state_requests(),
            Self::ExecutionWitness(rpc_db) => rpc_db.state_requests(),
        }
    }

    fn advance_subblock(&self) {
        match self {
            Self::Basic(rpc_db) => rpc_db.advance_subblock(),
            Self::ExecutionWitness(rpc_db) => rpc_db.advance_subblock(),
        }
    }

    fn update_state_diffs(&mut self, state_diffs: &BundleState) {
        match self {
            Self::Basic(rpc_db) => rpc_db.update_state_diffs(state_diffs),
            Self::ExecutionWitness(rpc_db) => rpc_db.update_state_diffs(state_diffs),
        }
    }

    fn store_cache(&self, file_path: &Path) -> Result<(), RpcDbError> {
        match self {
            Self::Basic(rpc_db) => rpc_db.store_cache(file_path),
            Self::ExecutionWitness(rpc_db) => rpc_db.store_cache(file_path),
        }
    }
}

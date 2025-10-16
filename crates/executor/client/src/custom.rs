use alloy_evm::{
    eth::{EthEvmBuilder, EthEvmContext},
    EthEvm,
};
use reth_evm::{precompiles::PrecompilesMap, Database, EvmEnv, EvmFactory};
use revm::{
    context::{
        result::{EVMError, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    handler::EthPrecompiles,
    inspector::NoOpInspector,
    Context, Inspector,
};
use revm_primitives::hardfork::SpecId;
use std::fmt::Debug;

#[derive(Debug, Default, Clone, Copy)]
pub struct CustomEvmFactory;

impl EvmFactory for CustomEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>>> = EthEvm<DB, I, Self::Precompiles>;
    type Context<DB: Database> = Context<BlockEnv, TxEnv, CfgEnv, DB>;
    type Tx = TxEnv;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        evm_builder(db, input).build()
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        evm_builder(db, input).activate_inspector(inspector).build()
    }
}

// create the evm builder
fn evm_builder<DB: Database>(db: DB, mut input: EvmEnv) -> EthEvmBuilder<DB, NoOpInspector> {
    #[allow(unused_mut)]
    let mut precompiles = PrecompilesMap::from(EthPrecompiles::default());

    #[cfg(target_os = "zkvm")]
    precompiles.map_precompiles(|address, p| {
        use alloy_evm::precompiles::Precompile;
        use reth_evm::precompiles::PrecompileInput;
        use revm::precompile::u64_to_address;
        use std::collections::HashMap;

        let addresses_to_names = HashMap::from([
            (u64_to_address(1), "ecrecover"),
            (u64_to_address(2), "sha256"),
            (u64_to_address(3), "ripemd160"),
            (u64_to_address(4), "identity"),
            (u64_to_address(5), "modexp"),
            (u64_to_address(6), "bn-add"),
            (u64_to_address(7), "bn-mul"),
            (u64_to_address(8), "bn-pair"),
            (u64_to_address(9), "blake2f"),
            (u64_to_address(10), "kzg-point-evaluation"),
        ]);

        let name = addresses_to_names.get(address).cloned().unwrap_or("unknown");

        let precompile = move |input: PrecompileInput<'_>| {
            println!("cycle-tracker-report-start: precompile-{name}");
            let result = p.call(input);
            println!("cycle-tracker-report-end: precompile-{name}");

            result
        };
        precompile.into()
    });

    // disable balance and nonce checks for replay
    input.cfg_env.disable_balance_check = true;
    input.cfg_env.disable_nonce_check = true;

    EthEvmBuilder::new(db, input).precompiles(precompiles)
}

use alloy_evm::{eth::EthEvmBuilder, EthEvm};
use kzg_rs::{Bytes32, Bytes48, KzgProof, KzgSettings};
use reth_evm::{precompiles::PrecompilesMap, Database, EvmEnv, EvmFactory};
use revm::{
    context::{
        result::{EVMError, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    inspector::NoOpInspector,
    precompile::{Crypto, PrecompileError, PrecompileSpecId, Precompiles},
    Context,
};
use revm_primitives::hardfork::SpecId;
use std::fmt::Debug;

#[derive(Debug, Default, Clone, Copy)]
pub struct CustomEvmFactory;

impl EvmFactory for CustomEvmFactory {
    type BlockEnv = BlockEnv;
    type Context<DB: Database> = Context<BlockEnv, TxEnv, CfgEnv, DB>;
    type Error<DBError: std::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type Evm<DB: Database, I: revm::Inspector<Self::Context<DB>>> = EthEvm<DB, I, PrecompilesMap>;
    type HaltReason = HaltReason;
    type Precompiles = PrecompilesMap;
    type Spec = SpecId;
    type Tx = TxEnv;

    fn create_evm<DB: Database>(
        &self,
        db: DB,
        input: EvmEnv,
    ) -> Self::Evm<DB, revm::inspector::NoOpInspector> {
        evm_builder(db, input).build()
    }

    fn create_evm_with_inspector<DB: Database, I: revm::Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        evm_builder(db, input).activate_inspector(inspector).build()
    }
}

#[derive(Debug)]
pub struct CustomCrypto {
    kzg_settings: KzgSettings,
}

impl Default for CustomCrypto {
    fn default() -> Self {
        Self { kzg_settings: KzgSettings::load_trusted_setup_file().unwrap() }
    }
}

impl Crypto for CustomCrypto {
    fn verify_kzg_proof(
        &self,
        z: &[u8; 32],
        y: &[u8; 32],
        commitment: &[u8; 48],
        proof: &[u8; 48],
    ) -> Result<(), PrecompileError> {
        if !KzgProof::verify_kzg_proof(
            &Bytes48(*commitment),
            &Bytes32(*z),
            &Bytes32(*y),
            &Bytes48(*proof),
            &self.kzg_settings,
        )
        .map_err(|err| PrecompileError::other(err.to_string()))?
        {
            return Err(PrecompileError::BlobVerifyKzgProofFailed);
        }

        Ok(())
    }
}

// create the evm builder
fn evm_builder<DB: Database>(db: DB, mut input: EvmEnv) -> EthEvmBuilder<DB, NoOpInspector> {
    #[allow(unused_mut)]
    let mut precompiles = PrecompilesMap::from_static(Precompiles::new(
        PrecompileSpecId::from_spec_id(input.cfg_env.spec),
    ));

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
            (u64_to_address(11), "bls-g1add"),
            (u64_to_address(12), "bls-g1msm"),
            (u64_to_address(13), "bls-g2add"),
            (u64_to_address(14), "bls-g2msm"),
            (u64_to_address(15), "bls-pairing"),
            (u64_to_address(16), "bls-map-fp-to-g1"),
            (u64_to_address(17), "bls-map-fp2-to-g2"),
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

    // disable nonce check for replay
    input.cfg_env.disable_nonce_check = true;

    EthEvmBuilder::new(db, input).precompiles(precompiles)
}

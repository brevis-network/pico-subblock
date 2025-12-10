use alloy_eips::{eip7840::BlobParams, BlobScheduleBlobParams};
use reth_chainspec::{
    mainnet::{MAINNET_BPO1_TIMESTAMP, MAINNET_BPO2_TIMESTAMP},
    BaseFeeParams, BaseFeeParamsKind, Chain, ChainSpec, EthereumHardfork,
    MAINNET_PRUNE_DELETE_LIMIT,
};

pub fn mainnet() -> ChainSpec {
    ChainSpec {
        chain: Chain::mainnet(),
        genesis: Default::default(),
        genesis_header: Default::default(),
        paris_block_and_final_difficulty: Default::default(),
        hardforks: EthereumHardfork::mainnet().into(),
        deposit_contract: Default::default(),
        base_fee_params: BaseFeeParamsKind::Constant(BaseFeeParams::ethereum()),
        prune_delete_limit: MAINNET_PRUNE_DELETE_LIMIT,
        blob_params: BlobScheduleBlobParams::default().with_scheduled([
            (MAINNET_BPO1_TIMESTAMP, BlobParams::bpo1()),
            (MAINNET_BPO2_TIMESTAMP, BlobParams::bpo2()),
        ]),
    }
}

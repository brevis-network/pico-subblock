#![allow(deprecated)]

use alloy_provider::{network::AnyNetwork, Provider as _, ReqwestProvider};
use clap::Parser;
use std::env;
use url::Url;

/// The arguments for configuring the chain data provider.
#[derive(Debug, Clone, Parser)]
pub struct ProviderArgs {
    /// The chain ID. If not provided, requires basic_rpc_url and debug_rpc_url to be provided.
    #[clap(long)]
    chain_id: Option<u64>,
    /// The basic rpc url used to fetch data about the block. If not provided, will use the
    /// BASIC_RPC_{chain_id} env var.
    #[clap(long)]
    basic_rpc_url: Option<Url>,
    /// The debug rpc url used to fetch data about the block. If not provided, will use the
    /// DEBUG_RPC_{chain_id} env var.
    #[clap(long)]
    debug_rpc_url: Option<Url>,
}

pub struct ProviderConfig {
    pub chain_id: u64,
    pub basic_rpc_url: Option<Url>,
    pub debug_rpc_url: Option<Url>,
}

impl ProviderArgs {
    pub async fn into_provider(self) -> eyre::Result<ProviderConfig> {
        // We don't need RPC when using cache with known chain ID, so we leave it as `Option<Url>`
        // here and decide on whether to panic later. On the other hand chain ID is always needed.
        let (chain_id, basic_rpc_url, debug_rpc_url) =
            match (self.chain_id, self.basic_rpc_url, self.debug_rpc_url) {
                (Some(chain_id), Some(basic_rpc_url), Some(debug_rpc_url)) => {
                    (chain_id, Some(basic_rpc_url), Some(debug_rpc_url))
                }
                (Some(chain_id), None, None) => {
                    match [
                        env::var(format!("BASIC_RPC_{chain_id}")),
                        env::var(format!("DEBUG_RPC_{chain_id}")),
                    ] {
                        [Ok(basic_rpc_url), Ok(debug_rpc_url)] => {
                            // We don't always need it but if the value exists it has to be valid.
                            (
                                chain_id,
                                Some(Url::parse(basic_rpc_url.as_str()).unwrap()),
                                Some(Url::parse(debug_rpc_url.as_str()).unwrap()),
                            )
                        }
                        _ => {
                            // Not having RPC is okay because we know chain ID.
                            (chain_id, None, None)
                        }
                    }
                }
                (None, Some(basic_rpc_url), Some(debug_rpc_url)) => {
                    // We can find out about chain ID from RPC.
                    let basic_provider: ReqwestProvider<AnyNetwork> =
                        ReqwestProvider::new_http(basic_rpc_url.clone());
                    let debug_provider: ReqwestProvider<AnyNetwork> =
                        ReqwestProvider::new_http(debug_rpc_url.clone());
                    let basic_chain_id = basic_provider.get_chain_id().await?;
                    let debug_chain_id = debug_provider.get_chain_id().await?;
                    assert_eq!(basic_chain_id, debug_chain_id);

                    (basic_chain_id, Some(basic_rpc_url), Some(debug_rpc_url))
                }
                _ => {
                    eyre::bail!(
                        "either --chain-id or --basic-rpc-url and --debug-rpc-url must be used"
                    )
                }
            };

        Ok(ProviderConfig { chain_id, basic_rpc_url, debug_rpc_url })
    }
}

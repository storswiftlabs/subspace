use clap::Parser;
use prometheus_client::registry::Registry;
use subspace_farmer::cluster::nats_client::NatsClient;

/// Arguments for cache
#[derive(Debug, Parser)]
pub(super) struct CacheArgs {
    // TODO: Paths
    /// Cache group to use, the same cache group must be also specified on corresponding controller
    #[arg(long, default_value = "default")]
    cache_group: String,
}

pub(super) async fn cache(
    _nats_client: NatsClient,
    _registry: &mut Registry,
    _cache_args: CacheArgs,
) -> anyhow::Result<()> {
    todo!()
}

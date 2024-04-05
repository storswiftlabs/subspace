mod cache;
mod controller;
mod farmer;
mod plotter;

use crate::commands::cluster::cache::{cache, CacheArgs};
use crate::commands::cluster::controller::{controller, ControllerArgs};
use crate::commands::cluster::farmer::{farmer, FarmerArgs};
use crate::commands::cluster::plotter::{plotter, PlotterArgs};
use crate::utils::shutdown_signal;
use anyhow::anyhow;
use async_nats::ServerAddr;
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use prometheus_client::registry::Registry;
use std::net::SocketAddr;
use subspace_farmer::cluster::nats_client::NatsClient;
use subspace_proof_of_space::Table;

/// Arguments for farmer
#[derive(Debug, Parser)]
pub(crate) struct ClusterArgs {
    /// Shared arguments for all subcommands
    #[clap(flatten)]
    shared_args: SharedArgs,
    /// Cluster subcommands
    #[clap(subcommand)]
    subcommand: ClusterSubcommand,
}

/// Shared arguments
#[derive(Debug, Parser)]
struct SharedArgs {
    /// NATS server address, typically in `nats://server1:port1` format, can be specified multiple
    /// times.
    ///
    /// NOTE: NATS must be configured for message sizes of 2MiB or larger (1MiB is the default).
    #[arg(long, alias = "nats-server")]
    nats_servers: Vec<ServerAddr>,
    /// Defines endpoints for the prometheus metrics server. It doesn't start without at least
    /// one specified endpoint. Format: 127.0.0.1:8080
    #[arg(long, aliases = ["metrics-endpoint", "metrics-endpoints"])]
    prometheus_listen_on: Vec<SocketAddr>,
}

/// Arguments for cluster
#[derive(Debug, Subcommand)]
enum ClusterSubcommand {
    /// Farming cluster controller
    Controller(ControllerArgs),
    /// Farming cluster farmer
    Farmer(FarmerArgs),
    /// Farming cluster plotter
    Plotter(PlotterArgs),
    /// Farming cluster cache
    Cache(CacheArgs),
}

pub(crate) async fn cluster<PosTable>(cluster_args: ClusterArgs) -> anyhow::Result<()>
where
    PosTable: Table,
{
    let signal = shutdown_signal();

    let nats_client = NatsClient::new(cluster_args.shared_args.nats_servers)
        .await
        .map_err(|error| anyhow!(error))?;
    let mut registry = Registry::default();

    let run_fut = async move {
        match cluster_args.subcommand {
            ClusterSubcommand::Controller(controller_args) => {
                controller(nats_client, &mut registry, controller_args).await
            }
            ClusterSubcommand::Farmer(farmer_args) => {
                farmer::<PosTable>(nats_client, &mut registry, farmer_args).await
            }
            ClusterSubcommand::Plotter(plotter_args) => {
                plotter::<PosTable>(nats_client, &mut registry, plotter_args).await
            }
            ClusterSubcommand::Cache(cache_args) => {
                cache(nats_client, &mut registry, cache_args).await
            }
        }
    };

    // TODO: Run
    // let _prometheus_worker = if !cluster_args.shared_args.prometheus_listen_on.is_empty() {
    //     let prometheus_task = start_prometheus_metrics_server(
    //         cluster_args.shared_args.prometheus_listen_on,
    //         RegistryAdapter::PrometheusClient(registry),
    //     )?;
    //
    //     let join_handle = tokio::spawn(prometheus_task);
    //     Some(AsyncJoinOnDrop::new(join_handle, true))
    // } else {
    //     None
    // };

    select! {
        // Signal future
        _ = signal.fuse() => {
            Ok(())
        },

        // Run future
        result = run_fut.fuse() => {
            result
        },
    }
}

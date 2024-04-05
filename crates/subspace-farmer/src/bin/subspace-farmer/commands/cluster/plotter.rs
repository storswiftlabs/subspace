use crate::commands::shared::PlottingThreadPriority;
use clap::Parser;
use prometheus_client::registry::Registry;
use std::num::NonZeroUsize;
use subspace_farmer::cluster::nats_client::NatsClient;
use subspace_proof_of_space::Table;

/// Arguments for plotter
#[derive(Debug, Parser)]
pub(super) struct PlotterArgs {
    /// Defines how many sectors farmer will download concurrently, allows to limit memory usage of
    /// the plotting process, defaults to `--sector-encoding-concurrency` + 1 to download future
    /// sector ahead of time.
    ///
    /// Increase will result in higher memory usage.
    #[arg(long)]
    sector_downloading_concurrency: Option<NonZeroUsize>,
    /// Defines how many sectors farmer will encode concurrently, defaults to 1 on UMA system and
    /// number of NUMA nodes on NUMA system or L3 cache groups on large CPUs. It is further
    /// restricted by
    /// `--sector-downloading-concurrency` and setting this option higher than
    /// `--sector-downloading-concurrency` will have no effect.
    ///
    /// Increase will result in higher memory usage.
    #[arg(long)]
    sector_encoding_concurrency: Option<NonZeroUsize>,
    /// Defines how many record farmer will encode in a single sector concurrently, defaults to one
    /// record per 2 cores, but not more than 8 in total. Higher concurrency means higher memory
    /// usage and typically more efficient CPU utilization.
    #[arg(long)]
    record_encoding_concurrency: Option<NonZeroUsize>,
    /// Size of one thread pool used for plotting, defaults to number of logical CPUs available
    /// on UMA system and number of logical CPUs available in NUMA node on NUMA system or L3 cache
    /// groups on large CPUs.
    ///
    /// Number of thread pools is defined by `--sector-encoding-concurrency` option, different
    /// thread pools might have different number of threads if NUMA nodes do not have the same size.
    ///
    /// Threads will be pinned to corresponding CPU cores at creation.
    #[arg(long)]
    plotting_thread_pool_size: Option<NonZeroUsize>,
    /// Specify exact CPU cores to be used for plotting bypassing any custom logic farmer might use
    /// otherwise. It replaces both `--sector-encoding-concurrency` and
    /// `--plotting-thread-pool-size` options if specified. Requires `--replotting-cpu-cores` to be
    /// specified with the same number of CPU cores groups (or not specified at all, in which case
    /// it'll use the same thread pool as plotting).
    ///
    /// Cores are coma-separated, with whitespace separating different thread pools/encoding
    /// instances. For example "0,1 2,3" will result in two sectors being encoded at the same time,
    /// each with a pair of CPU cores.
    #[arg(long, conflicts_with_all = & ["sector_encoding_concurrency", "plotting_thread_pool_size"])]
    plotting_cpu_cores: Option<String>,
    /// Plotting thread priority, by default de-prioritizes plotting threads in order to make sure
    /// farming is successful and computer can be used comfortably for other things
    #[arg(long, default_value_t = PlottingThreadPriority::Min)]
    plotting_thread_priority: PlottingThreadPriority,
}

pub(super) async fn plotter<PosTable>(
    _nats_client: NatsClient,
    _registry: &mut Registry,
    _plotter_args: PlotterArgs,
) -> anyhow::Result<()>
where
    PosTable: Table,
{
    todo!()
}

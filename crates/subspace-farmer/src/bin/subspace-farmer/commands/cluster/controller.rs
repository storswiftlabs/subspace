use crate::commands::cluster::farmer::FARMER_IDENTIFICATION_NOTIFICATION_INTERVAL;
use crate::commands::shared::derive_libp2p_keypair;
use crate::commands::shared::network::{configure_network, NetworkArgs};
use anyhow::anyhow;
use async_lock::RwLock as AsyncRwLock;
use backoff::ExponentialBackoff;
use clap::{Parser, ValueHint};
use futures::stream::FuturesUnordered;
use futures::{select, FutureExt, StreamExt};
use parity_scale_codec::Decode;
use prometheus_client::registry::Registry;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::{pending, Future};
use std::path::PathBuf;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subspace_core_primitives::crypto::kzg::{embedded_kzg_settings, Kzg};
use subspace_farmer::cluster::controller::ClusterControllerFarmerIdentifyBroadcast;
use subspace_farmer::cluster::farmer::{ClusterFarm, ClusterFarmerIdentifyFarmNotification};
use subspace_farmer::cluster::nats_client::{GenericNotification, NatsClient};
use subspace_farmer::farm::FarmId;
use subspace_farmer::farmer_cache::FarmerCache;
use subspace_farmer::utils::farmer_piece_getter::{DsnCacheRetryPolicy, FarmerPieceGetter};
use subspace_farmer::utils::piece_validator::SegmentCommitmentPieceValidator;
use subspace_farmer::utils::plotted_pieces::PlottedPieces;
use subspace_farmer::utils::run_future_in_dedicated_thread;
use subspace_farmer::{Identity, NodeClient, NodeRpcClient};
use subspace_networking::utils::piece_provider::PieceProvider;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

/// Get piece retry attempts number.
const PIECE_GETTER_MAX_RETRIES: u16 = 7;
/// Defines initial duration between get_piece calls.
const GET_PIECE_INITIAL_INTERVAL: Duration = Duration::from_secs(5);
/// Defines max duration between get_piece calls.
const GET_PIECE_MAX_INTERVAL: Duration = Duration::from_secs(40);

type FarmIndex = u16;

#[derive(Debug)]
struct KnownFarm {
    farm_id: FarmId,
    last_identification: Instant,
}

#[derive(Debug, Default)]
struct KnownFarms {
    known_farms: HashMap<FarmIndex, KnownFarm>,
}

impl KnownFarms {
    /// Returns new allocated farm index if farm was not known, `None` otherwise
    fn insert_or_update(&mut self, farm_id: FarmId) -> Option<FarmIndex> {
        if self
            .known_farms
            .iter_mut()
            .any(|(_farm_index, known_farm)| {
                if known_farm.farm_id == farm_id {
                    known_farm.last_identification = Instant::now();
                    true
                } else {
                    false
                }
            })
        {
            None
        } else {
            for farm_index in FarmIndex::MIN..=FarmIndex::MAX {
                if let Entry::Vacant(entry) = self.known_farms.entry(farm_index) {
                    entry.insert(KnownFarm {
                        farm_id,
                        last_identification: Instant::now(),
                    });

                    return Some(farm_index);
                }
            }

            warn!(%farm_id, max_supported_farm_index = %FarmIndex::MAX, "Too many farms, ignoring");
            None
        }
    }

    fn remove(&mut self, farm_index: FarmIndex) {
        self.known_farms.remove(&farm_index);
    }
}

// TODO: Piece request processing concurrency
/// Arguments for controller
#[derive(Debug, Parser)]
pub(super) struct ControllerArgs {
    /// Base path where to store P2P network identity
    #[arg(long, value_hint = ValueHint::DirPath)]
    base_path: PathBuf,
    /// WebSocket RPC URL of the Subspace node to connect to
    #[arg(long, value_hint = ValueHint::Url, default_value = "ws://127.0.0.1:9944")]
    node_rpc_url: String,
    /// Cache group managed by this controller, each controller must have its dedicated cache group.
    ///
    /// It is strongly recommended to use alphanumeric values for cache group, the same cache group
    /// must be also specified on corresponding caches.
    #[arg(long, default_value = "default")]
    cache_group: String,
    /// Network parameters
    #[clap(flatten)]
    network_args: NetworkArgs,
    /// Sets some flags that are convenient during development, currently `--allow-private-ips`
    #[arg(long)]
    dev: bool,
    /// Run temporary controller identity
    #[arg(long, conflicts_with = "base_path")]
    tmp: bool,
}

pub(super) async fn controller(
    nats_client: NatsClient,
    registry: &mut Registry,
    controller_args: ControllerArgs,
) -> anyhow::Result<()> {
    let ControllerArgs {
        mut base_path,
        node_rpc_url,
        cache_group,
        mut network_args,
        dev,
        tmp,
    } = controller_args;

    // Override flags with `--dev`
    network_args.allow_private_ips = network_args.allow_private_ips || dev;

    let _tmp_directory = if tmp {
        let tmp_directory = tempfile::Builder::new()
            .prefix("subspace-cluster-controller-")
            .tempdir()?;

        base_path = tmp_directory.as_ref().to_path_buf();

        Some(tmp_directory)
    } else {
        if base_path == PathBuf::default() {
            return Err(anyhow!("--base-path must be specified explicitly"));
        }

        None
    };

    let plotted_pieces = Arc::new(AsyncRwLock::new(PlottedPieces::<FarmIndex>::default()));

    info!(url = %node_rpc_url, "Connecting to node RPC");
    let node_client = NodeRpcClient::new(&node_rpc_url).await?;

    let farmer_app_info = node_client
        .farmer_app_info()
        .await
        .map_err(|error| anyhow!(error))?;

    let identity = Identity::open_or_create(&base_path)
        .map_err(|error| anyhow!("Failed to open or create identity: {error}"))?;
    let keypair = derive_libp2p_keypair(identity.secret_key());
    let peer_id = keypair.public().to_peer_id();
    let instance = peer_id.to_string();

    let (farmer_cache, farmer_cache_worker) = FarmerCache::new(node_client.clone(), peer_id);

    // TODO: Metrics

    let (node, mut node_runner) = {
        if network_args.bootstrap_nodes.is_empty() {
            network_args
                .bootstrap_nodes
                .clone_from(&farmer_app_info.dsn_bootstrap_nodes);
        }

        configure_network(
            hex::encode(farmer_app_info.genesis_hash),
            &base_path,
            keypair,
            network_args,
            Arc::downgrade(&plotted_pieces),
            node_client.clone(),
            farmer_cache.clone(),
            Some(registry),
        )?
    };

    let kzg = Kzg::new(embedded_kzg_settings());
    let validator = Some(SegmentCommitmentPieceValidator::new(
        node.clone(),
        node_client.clone(),
        kzg.clone(),
    ));
    let piece_provider = PieceProvider::new(node.clone(), validator.clone());

    let piece_getter = FarmerPieceGetter::new(
        piece_provider,
        farmer_cache.clone(),
        node_client.clone(),
        Arc::clone(&plotted_pieces),
        DsnCacheRetryPolicy {
            max_retries: PIECE_GETTER_MAX_RETRIES,
            backoff: ExponentialBackoff {
                initial_interval: GET_PIECE_INITIAL_INTERVAL,
                max_interval: GET_PIECE_MAX_INTERVAL,
                // Try until we get a valid piece
                max_elapsed_time: None,
                multiplier: 1.75,
                ..ExponentialBackoff::default()
            },
        },
    );

    let farmer_cache_worker_fut = run_future_in_dedicated_thread(
        {
            let future = farmer_cache_worker.run(piece_getter.downgrade());

            move || future
        },
        "controller-cache-worker".to_string(),
    )?;

    // TODO: Farm identification
    let farm_fut = run_future_in_dedicated_thread(
        move || async move {
            let mut known_farms = KnownFarms::default();
            // Initialize with pending future so it never ends
            let mut farms = FuturesUnordered::<
                Pin<Box<dyn Future<Output = (FarmIndex, anyhow::Result<()>)>>>,
            >::from_iter([Box::pin(pending()) as Pin<Box<_>>]);

            let farmer_identify_subscription = pin!(nats_client
                .subscribe(ClusterFarmerIdentifyFarmNotification::SUBSCRIPTION_SUBJECT)
                .await?
                .filter_map(move |message| async move {
                    ClusterFarmerIdentifyFarmNotification::decode(&mut message.payload.as_ref())
                        .ok()
                }));

            // Request farmer to identify themselves
            if let Err(error) = nats_client
                .broadcast(&ClusterControllerFarmerIdentifyBroadcast, &instance)
                .await
            {
                warn!(%error, "Failed to send farmer identification broadcast");
            }

            let mut farmer_identify_subscription = farmer_identify_subscription.fuse();
            let mut farm_pruning_interval = tokio::time::interval_at(
                (Instant::now() + FARMER_IDENTIFICATION_NOTIFICATION_INTERVAL * 2).into(),
                FARMER_IDENTIFICATION_NOTIFICATION_INTERVAL * 2,
            );
            farm_pruning_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            select! {
                (farm_index, result) = farms.select_next_some() => {
                    known_farms.remove(farm_index);
                    plotted_pieces.write().await.delete_farm(farm_index);
                    // TODO: Remove from other things

                    match result {
                        Ok(()) => {
                            info!(%farm_index, "Farm exited successfully");
                        }
                        Err(error) => {
                            error!(%farm_index, %error, "Farm exited with error");
                        }
                    }
                }
                maybe_identify_message = farmer_identify_subscription.next() => {
                    if let Some(identify_message) = maybe_identify_message {
                        if let Some(farm_index) = known_farms.insert_or_update(identify_message.farm_id) {
                            let farm = ClusterFarm::new(
                                identify_message.farm_id,
                                identify_message.total_sectors_count,
                                nats_client.clone(),
                            );
                            // TODO: Use this farm
                        }
                    } else {
                        return Err(anyhow!("Farmer identify stream ended"));
                    }
                }
                _ = farm_pruning_interval.tick().fuse() => {
                    // TODO: Prune farms that were not seen for a long time
                }
            }

            anyhow::Ok(())
        },
        "controller-farm".to_string(),
    )?;

    let networking_fut = run_future_in_dedicated_thread(
        move || async move { node_runner.run().await },
        "controller-networking".to_string(),
    )?;

    // This defines order in which things are dropped
    let networking_fut = networking_fut;
    let farm_fut = farm_fut;
    let farmer_cache_worker_fut = farmer_cache_worker_fut;

    let networking_fut = pin!(networking_fut);
    let farm_fut = pin!(farm_fut);
    let farmer_cache_worker_fut = pin!(farmer_cache_worker_fut);

    select! {
        // Networking future
        _ = networking_fut.fuse() => {
            info!("Node runner exited.")
        },

        // Farm future
        result = farm_fut.fuse() => {
            result??;
        },

        // Piece cache worker future
        _ = farmer_cache_worker_fut.fuse() => {
            info!("Farmer cache worker exited.")
        },
    }

    anyhow::Ok(())
}

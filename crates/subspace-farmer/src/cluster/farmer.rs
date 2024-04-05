use crate::cluster::controller::ClusterControllerFarmerIdentifyBroadcast;
use crate::cluster::nats_client::{GenericBroadcast, GenericNotification, NatsClient};
use crate::farm::{
    Farm, FarmError, FarmId, FarmingNotification, HandlerFn, HandlerId, MaybePieceStoredResult,
    PieceCache, PieceCacheOffset, PieceReader, PlotCache, SectorUpdate,
};
use async_nats::SubscribeError;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{select, stream, FutureExt, Stream, StreamExt};
use parity_scale_codec::{Decode, Encode};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use subspace_core_primitives::{Piece, PieceIndex, SectorIndex};
use subspace_farmer_components::plotting::PlottedSector;
use subspace_networking::libp2p::kad::RecordKey;
use subspace_rpc_primitives::SolutionResponse;
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace, warn};

/// Broadcast messages with identification details by farmers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterFarmerIdentifyFarmNotification {
    /// Farm ID
    pub farm_id: FarmId,
    /// Total number of sectors in the farm
    pub total_sectors_count: SectorIndex,
    // TODO: Farm signature to make sure farm changes are reflected on the controller
}

impl GenericNotification for ClusterFarmerIdentifyFarmNotification {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.farmer.*.identify";
}

#[derive(Debug)]
struct DummyPieceCache;

#[async_trait]
impl PieceCache for DummyPieceCache {
    fn max_num_elements(&self) -> usize {
        0
    }

    async fn contents(
        &self,
    ) -> Box<dyn Stream<Item = (PieceCacheOffset, Option<PieceIndex>)> + Unpin + '_> {
        Box::new(stream::empty())
    }

    async fn write_piece(
        &self,
        _offset: PieceCacheOffset,
        _piece_index: PieceIndex,
        _piece: &Piece,
    ) -> Result<(), FarmError> {
        Err("Can't write pieces into empty cache".into())
    }

    async fn read_piece_index(
        &self,
        _offset: PieceCacheOffset,
    ) -> Result<Option<PieceIndex>, FarmError> {
        Ok(None)
    }

    async fn read_piece(&self, _offset: PieceCacheOffset) -> Result<Option<Piece>, FarmError> {
        Ok(None)
    }
}

#[derive(Debug)]
struct DummyPlotCache;

#[async_trait]
impl PlotCache for DummyPlotCache {
    async fn is_piece_maybe_stored(
        &self,
        _key: &RecordKey,
    ) -> Result<MaybePieceStoredResult, FarmError> {
        Ok(MaybePieceStoredResult::No)
    }

    async fn try_store_piece(
        &self,
        _piece_index: PieceIndex,
        _piece: &Piece,
    ) -> Result<bool, FarmError> {
        Ok(false)
    }

    async fn read_piece(&self, _key: &RecordKey) -> Result<Option<Piece>, FarmError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct ClusterFarm {
    farm_id: FarmId,
    total_sectors_count: SectorIndex,
    nats_client: NatsClient,
}

#[async_trait(?Send)]
impl Farm for ClusterFarm {
    fn id(&self) -> &FarmId {
        &self.farm_id
    }

    fn total_sectors_count(&self) -> SectorIndex {
        self.total_sectors_count
    }

    async fn plotted_sectors_count(&self) -> Result<SectorIndex, FarmError> {
        todo!()
    }

    async fn plotted_sectors(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<PlottedSector, FarmError>> + Unpin + '_>, FarmError>
    {
        todo!()
    }

    fn piece_cache(&self) -> Arc<dyn PieceCache + 'static> {
        Arc::new(DummyPieceCache)
    }

    fn plot_cache(&self) -> Arc<dyn PlotCache + 'static> {
        Arc::new(DummyPlotCache)
    }

    fn piece_reader(&self) -> Arc<dyn PieceReader + 'static> {
        todo!()
    }

    fn on_sector_update(
        &self,
        callback: HandlerFn<(SectorIndex, SectorUpdate)>,
    ) -> Box<dyn HandlerId> {
        todo!()
    }

    fn on_farming_notification(
        &self,
        callback: HandlerFn<FarmingNotification>,
    ) -> Box<dyn HandlerId> {
        todo!()
    }

    fn on_solution(&self, callback: HandlerFn<SolutionResponse>) -> Box<dyn HandlerId> {
        todo!()
    }

    fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        todo!()
    }
}

impl ClusterFarm {
    pub fn new(farm_id: FarmId, total_sectors_count: SectorIndex, nats_client: NatsClient) -> Self {
        Self {
            farm_id,
            total_sectors_count,
            nats_client,
        }
    }
}

/// Details about the farm
#[derive(Debug, Copy, Clone)]
pub struct FarmDetails {
    /// Farm ID
    pub farm_id: FarmId,
    /// Total number of sectors in the farm
    pub total_sectors_count: SectorIndex,
}

pub async fn farmer_service(
    nats_client: &NatsClient,
    farms: &[FarmDetails],
    identification_notification_interval: Duration,
) -> Result<(), SubscribeError> {
    // Listen for farmer identification broadcast from controller and publish identification
    // broadcast in response, also send periodic notifications reminding that farm exists
    let identify_fut = async {
        let mut subscription = nats_client
            .subscribe(ClusterControllerFarmerIdentifyBroadcast::SUBSCRIPTION_SUBJECT)
            .await?
            .fuse();
        let mut interval = tokio::time::interval(identification_notification_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                maybe_message = subscription.next() => {
                    if let Some(message) = maybe_message {
                        trace!(?message, "Farmer received identify broadcast message");

                        send_identify_notification(nats_client, farms).await;
                        interval.reset();
                    } else {
                        debug!("Identify broadcast stream ended");
                        break;
                    }
                }
                _ = interval.tick().fuse() => {
                    trace!("Farmer self-identification");

                    send_identify_notification(nats_client, farms).await;
                }
            }
        }

        Ok(())
    };

    select! {
        // Run future
        result = identify_fut.fuse() => {
            result
        },
    }
}

async fn send_identify_notification(nats_client: &NatsClient, farms: &[FarmDetails]) {
    farms
        .iter()
        .map(|farm_details| async move {
            if let Err(error) = nats_client
                .notification(
                    &ClusterFarmerIdentifyFarmNotification {
                        farm_id: farm_details.farm_id,
                        total_sectors_count: farm_details.total_sectors_count,
                    },
                    Some(&farm_details.farm_id.to_string()),
                )
                .await
            {
                warn!(
                    farm_id = %farm_details.farm_id,
                    %error,
                    "Failed to send farmer identify notification"
                );
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;
}

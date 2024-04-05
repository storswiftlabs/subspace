use crate::cluster::nats_client::NatsClient;
use crate::plotter::{Plotter, SectorPlottingProgress};
use async_trait::async_trait;
use futures::Sink;
use std::error::Error;
use subspace_core_primitives::{PublicKey, SectorIndex};
use subspace_farmer_components::FarmerProtocolInfo;

/// Cluster plotter
// TODO: Limit number of sectors that can be plotted concurrently in order to limit memory usage
pub struct ClusterPlotter {
    nats_client: NatsClient,
}

#[async_trait]
impl Plotter for ClusterPlotter {
    async fn plot_sector<PS>(
        &self,
        public_key: PublicKey,
        sector_index: SectorIndex,
        farmer_protocol_info: FarmerProtocolInfo,
        pieces_in_sector: u16,
        replotting: bool,
        mut progress_sender: PS,
    ) where
        PS: Sink<SectorPlottingProgress> + Unpin + Send + 'static,
        PS::Error: Error,
    {
        // TODO
        todo!()
    }
}

impl ClusterPlotter {
    /// Create new instance
    pub fn new(nats_client: NatsClient) -> Self {
        Self { nats_client }
    }
}

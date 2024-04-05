use crate::cluster::controller::{
    ClusterControllerArchivedSegmentHeaderBroadcast, ClusterControllerFarmerAppInfoRequest,
    ClusterControllerPieceRequest, ClusterControllerRewardSignatureNotification,
    ClusterControllerRewardSigningBroadcast, ClusterControllerSegmentHeadersRequest,
    ClusterControllerSlotInfoBroadcast, ClusterControllerSolutionNotification,
};
use crate::cluster::nats_client::{GenericBroadcast, NatsClient};
use crate::node_client::{Error as RpcError, Error, NodeClient};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use parity_scale_codec::Decode;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use subspace_core_primitives::{Piece, PieceIndex, SegmentHeader, SegmentIndex};
use subspace_rpc_primitives::{
    FarmerAppInfo, RewardSignatureResponse, RewardSigningInfo, SlotInfo, SolutionResponse,
};

/// [`NodeClient`] used in cluster environment that connects to node through a controller instead
/// of to the node directly
#[derive(Debug, Clone)]
pub struct ClusterNodeClient {
    client: NatsClient,
    // Store last slot info instance that can be used to send solution response to (some instances
    // may be not synced and not able to receive solution responses)
    last_slot_info_instance: Arc<Mutex<String>>,
}

impl ClusterNodeClient {
    /// Create a new instance
    pub fn new(client: NatsClient) -> Self {
        Self {
            client,
            last_slot_info_instance: Arc::default(),
        }
    }
}

#[async_trait]
impl NodeClient for ClusterNodeClient {
    async fn farmer_app_info(&self) -> Result<FarmerAppInfo, Error> {
        Ok(self
            .client
            .request(&ClusterControllerFarmerAppInfoRequest, None)
            .await?)
    }

    async fn subscribe_slot_info(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = SlotInfo> + Send + 'static>>, RpcError> {
        let subscription = self
            .client
            .subscribe(ClusterControllerSlotInfoBroadcast::SUBSCRIPTION_SUBJECT)
            .await?;
        let last_slot_info_instance = Arc::clone(&self.last_slot_info_instance);

        Ok(Box::pin(subscription.filter_map(move |message| {
            let maybe_slot_info =
                ClusterControllerSlotInfoBroadcast::decode(&mut message.payload.as_ref())
                    .ok()
                    .map(|message| {
                        *last_slot_info_instance.lock() = message.instance;

                        message.slot_info
                    });

            async move { maybe_slot_info }
        })))
    }

    async fn submit_solution_response(
        &self,
        solution_response: SolutionResponse,
    ) -> Result<(), RpcError> {
        let last_slot_info_instance = self.last_slot_info_instance.lock().clone();
        Ok(self
            .client
            .notification(
                &ClusterControllerSolutionNotification { solution_response },
                Some(&last_slot_info_instance),
            )
            .await?)
    }

    async fn subscribe_reward_signing(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RewardSigningInfo> + Send + 'static>>, RpcError> {
        let subscription = self
            .client
            .subscribe(ClusterControllerRewardSigningBroadcast::SUBSCRIPTION_SUBJECT)
            .await?;

        Ok(Box::pin(subscription.filter_map(|message| {
            let maybe_slot_info =
                ClusterControllerRewardSigningBroadcast::decode(&mut message.payload.as_ref())
                    .ok()
                    .map(|message| message.reward_signing_info);

            async move { maybe_slot_info }
        })))
    }

    /// Submit a block signature
    async fn submit_reward_signature(
        &self,
        reward_signature: RewardSignatureResponse,
    ) -> Result<(), RpcError> {
        let last_slot_info_instance = self.last_slot_info_instance.lock().clone();
        Ok(self
            .client
            .notification(
                &ClusterControllerRewardSignatureNotification { reward_signature },
                Some(&last_slot_info_instance),
            )
            .await?)
    }

    async fn subscribe_archived_segment_headers(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = SegmentHeader> + Send + 'static>>, RpcError> {
        let subscription = self
            .client
            .subscribe(ClusterControllerArchivedSegmentHeaderBroadcast::SUBSCRIPTION_SUBJECT)
            .await?;

        Ok(Box::pin(subscription.filter_map(|message| {
            let maybe_slot_info = ClusterControllerArchivedSegmentHeaderBroadcast::decode(
                &mut message.payload.as_ref(),
            )
            .ok()
            .map(|message| message.archived_segment_header);

            async move { maybe_slot_info }
        })))
    }

    async fn segment_headers(
        &self,
        segment_indices: Vec<SegmentIndex>,
    ) -> Result<Vec<Option<SegmentHeader>>, RpcError> {
        Ok(self
            .client
            .request(
                &ClusterControllerSegmentHeadersRequest { segment_indices },
                None,
            )
            .await?)
    }

    async fn piece(&self, piece_index: PieceIndex) -> Result<Option<Piece>, RpcError> {
        Ok(self
            .client
            .request(&ClusterControllerPieceRequest { piece_index }, None)
            .await?)
    }

    async fn acknowledge_archived_segment_header(
        &self,
        _segment_index: SegmentIndex,
    ) -> Result<(), Error> {
        // Acknowledgement is unnecessary/unsupported
        Ok(())
    }
}

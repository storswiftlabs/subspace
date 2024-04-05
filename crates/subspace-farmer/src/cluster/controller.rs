use crate::cluster::nats_client::{
    GenericBroadcast, GenericNotification, GenericRequest, NatsClient,
};
use crate::NodeClient;
use anyhow::anyhow;
use async_nats::HeaderValue;
use futures::{select, FutureExt, StreamExt};
use parity_scale_codec::{Decode, Encode};
use std::time::{Duration, Instant};
use subspace_core_primitives::{Piece, PieceIndex, SegmentHeader, SegmentIndex};
use subspace_farmer_components::PieceGetter;
use subspace_rpc_primitives::{
    FarmerAppInfo, RewardSignatureResponse, RewardSigningInfo, SlotInfo, SolutionResponse,
};
use tracing::{debug, trace, warn};

const FARMER_APP_INFO_DEDUPLICATION_WINDOW: Duration = Duration::from_secs(1);

/// Broadcast messages sent by controllers requesting farmers to identify themselves
#[derive(Debug, Copy, Clone, Encode, Decode)]
pub struct ClusterControllerFarmerIdentifyBroadcast;

impl GenericBroadcast for ClusterControllerFarmerIdentifyBroadcast {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.*.farmer.identify";
}

/// Broadcast messages with slot info sent by controllers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerSlotInfoBroadcast {
    pub slot_info: SlotInfo,
    pub instance: String,
}

impl GenericBroadcast for ClusterControllerSlotInfoBroadcast {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.slot-info";

    fn deterministic_message_id(&self) -> Option<HeaderValue> {
        // TODO: Depending on answer in `https://github.com/nats-io/nats.docs/issues/663` this might
        //  be simplified to just a slot number
        Some(HeaderValue::from(
            format!("slot-info-{}", self.slot_info.slot_number).as_str(),
        ))
    }
}

/// Broadcast messages with reward signing info by controllers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerRewardSigningBroadcast {
    pub reward_signing_info: RewardSigningInfo,
}

impl GenericBroadcast for ClusterControllerRewardSigningBroadcast {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.reward-signing-info";
}

/// Broadcast messages with archived segment headers by controllers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerArchivedSegmentHeaderBroadcast {
    pub archived_segment_header: SegmentHeader,
}

impl GenericBroadcast for ClusterControllerArchivedSegmentHeaderBroadcast {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.archived-segment-header";

    fn deterministic_message_id(&self) -> Option<HeaderValue> {
        // TODO: Depending on answer in `https://github.com/nats-io/nats.docs/issues/663` this might
        //  be simplified to just a segment index
        Some(HeaderValue::from(
            format!(
                "archived-segment-{}",
                self.archived_segment_header.segment_index()
            )
            .as_str(),
        ))
    }
}

/// Notification messages with solution by farmers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerSolutionNotification {
    pub solution_response: SolutionResponse,
}

impl GenericNotification for ClusterControllerSolutionNotification {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.*.solution";
}

/// Notification messages with reward signature by farmers
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerRewardSignatureNotification {
    pub reward_signature: RewardSignatureResponse,
}

impl GenericNotification for ClusterControllerRewardSignatureNotification {
    const SUBSCRIPTION_SUBJECT: &'static str = "subspace.controller.*.reward-signature";
}

/// Request farmer app info from controller
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerFarmerAppInfoRequest;

impl GenericRequest for ClusterControllerFarmerAppInfoRequest {
    const SUBJECT: &'static str = "subspace.controller.farmer-app-info";
    type Response = FarmerAppInfo;
}

/// Request segment headers with specified segment indices
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerSegmentHeadersRequest {
    pub segment_indices: Vec<SegmentIndex>,
}

impl GenericRequest for ClusterControllerSegmentHeadersRequest {
    const SUBJECT: &'static str = "subspace.controller.segment-headers";
    type Response = Vec<Option<SegmentHeader>>;
}

/// Request piece with specified index
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterControllerPieceRequest {
    pub piece_index: PieceIndex,
}

impl GenericRequest for ClusterControllerPieceRequest {
    const SUBJECT: &'static str = "subspace.controller.piece-from-node";
    type Response = Option<Piece>;
}

// TODO: Make many of internal futures being able to process requests concurrently
pub async fn controller_service<NC, PG>(
    nats_client: &NatsClient,
    node_client: NC,
    piece_getter: PG,
    instance: &str,
) -> anyhow::Result<()>
where
    NC: NodeClient,
    PG: PieceGetter,
{
    // TODO: Farmer identify handling

    let slot_info_broadcasting_fut = async {
        let mut slot_info_notifications = node_client
            .subscribe_slot_info()
            .await
            .map_err(|error| anyhow!("Failed to subscribe to slot info notifications: {error}"))?;

        while let Some(slot_info) = slot_info_notifications.next().await {
            debug!(?slot_info, "New slot");

            let slot = slot_info.slot_number;

            if let Err(error) = nats_client
                .broadcast(
                    &ClusterControllerSlotInfoBroadcast {
                        slot_info,
                        instance: instance.to_string(),
                    },
                    instance,
                )
                .await
            {
                warn!(%slot, %error, "Failed to broadcast slot info");
            }
        }

        Ok(())
    };

    let reward_signing_broadcasting_fut = async {
        let mut reward_signing_notifications = node_client
            .subscribe_reward_signing()
            .await
            .map_err(|error| {
                anyhow!("Failed to subscribe to reward signing notifications: {error}")
            })?;

        while let Some(reward_signing_info) = reward_signing_notifications.next().await {
            trace!(?reward_signing_info, "New reward signing notification");

            if let Err(error) = nats_client
                .broadcast(
                    &ClusterControllerRewardSigningBroadcast {
                        reward_signing_info,
                    },
                    instance,
                )
                .await
            {
                warn!(%error, "Failed to broadcast reward signing info");
            }
        }

        Ok(())
    };

    let archived_segment_headers_broadcasting_fut = async {
        let mut archived_segments_notifications = node_client
            .subscribe_archived_segment_headers()
            .await
            .map_err(|error| {
                anyhow!("Failed to subscribe to archived segment header notifications: {error}")
            })?;

        while let Some(archived_segment_header) = archived_segments_notifications.next().await {
            trace!(
                ?archived_segment_header,
                "New archived archived segment header notification"
            );

            node_client
                .acknowledge_archived_segment_header(archived_segment_header.segment_index())
                .await
                .map_err(|error| {
                    anyhow!("Failed to acknowledge archived segment header: {error}")
                })?;

            if let Err(error) = nats_client
                .broadcast(
                    &ClusterControllerArchivedSegmentHeaderBroadcast {
                        archived_segment_header,
                    },
                    instance,
                )
                .await
            {
                warn!(%error, "Failed to broadcast archived segment header info");
            }
        }

        Ok(())
    };

    let solution_response_forwarding_fut = async {
        let mut subscription = nats_client
            .subscribe(
                ClusterControllerSolutionNotification::SUBSCRIPTION_SUBJECT.replace('*', instance),
            )
            .await
            .map_err(|error| anyhow!("Failed to subscribe to solution notifications: {error}"))?;

        while let Some(message) = subscription.next().await {
            let notification = match ClusterControllerSolutionNotification::decode(
                &mut message.payload.as_ref(),
            ) {
                Ok(notification) => notification,
                Err(error) => {
                    warn!(
                        %error,
                        message = %hex::encode(message.payload),
                        "Failed to decode solution notification"
                    );
                    continue;
                }
            };

            debug!(?notification, "Solution notification");

            if let Err(error) = node_client
                .submit_solution_response(notification.solution_response)
                .await
            {
                warn!(%error, "Failed to send solution response");
            }
        }

        Ok(())
    };

    let reward_signature_forwarding_fut = async {
        let mut subscription = nats_client
            .subscribe(
                ClusterControllerRewardSignatureNotification::SUBSCRIPTION_SUBJECT
                    .replace('*', instance),
            )
            .await
            .map_err(|error| {
                anyhow!("Failed to subscribe to reward signature notifications: {error}")
            })?;

        while let Some(message) = subscription.next().await {
            let notification = match ClusterControllerRewardSignatureNotification::decode(
                &mut message.payload.as_ref(),
            ) {
                Ok(notification) => notification,
                Err(error) => {
                    warn!(
                        %error,
                        message = %hex::encode(message.payload),
                        "Failed to decode reward signature notification"
                    );
                    continue;
                }
            };

            debug!(?notification, "Reward signature notification");

            if let Err(error) = node_client
                .submit_reward_signature(notification.reward_signature)
                .await
            {
                warn!(%error, "Failed to send reward signature");
            }
        }

        Ok(())
    };

    let farmer_app_info_responder_fut = async {
        let mut subscription = nats_client
            .queue_subscribe(
                ClusterControllerFarmerAppInfoRequest::SUBJECT,
                "subspace.controller".to_string(),
            )
            .await
            .map_err(|error| anyhow!("Failed to subscribe to farmer app info requests: {error}"))?;

        let mut last_farmer_app_info: <ClusterControllerFarmerAppInfoRequest as GenericRequest>::Response = node_client
            .farmer_app_info()
            .await
            .map_err(|error| anyhow!("Failed to get farmer app info: {error}"))?;
        let mut last_farmer_app_info_request = Instant::now();

        while let Some(message) = subscription.next().await {
            trace!("Farmer app info request");

            if last_farmer_app_info_request.elapsed() > FARMER_APP_INFO_DEDUPLICATION_WINDOW {
                match node_client.farmer_app_info().await {
                    Ok(new_last_farmer_app_info) => {
                        last_farmer_app_info = new_last_farmer_app_info;
                        last_farmer_app_info_request = Instant::now();
                    }
                    Err(error) => {
                        warn!(%error, "Failed to get farmer app info");
                    }
                }
            }

            if let Some(reply_subject) = message.reply {
                if let Err(error) = nats_client
                    .publish(reply_subject, last_farmer_app_info.encode().into())
                    .await
                {
                    warn!(%error, "Failed to send farmer app info response");
                }
            }
        }

        Ok(())
    };

    let segment_headers_responder_fut = async {
        let mut subscription = nats_client
            .queue_subscribe(
                ClusterControllerSegmentHeadersRequest::SUBJECT,
                "subspace.controller".to_string(),
            )
            .await
            .map_err(|error| anyhow!("Failed to subscribe to segment headers requests: {error}"))?;

        let mut last_request_response = None::<(
            ClusterControllerSegmentHeadersRequest,
            <ClusterControllerSegmentHeadersRequest as GenericRequest>::Response,
        )>;

        while let Some(message) = subscription.next().await {
            let request =
                match ClusterControllerSegmentHeadersRequest::decode(&mut message.payload.as_ref())
                {
                    Ok(request) => request,
                    Err(error) => {
                        warn!(
                            %error,
                            message = %hex::encode(message.payload),
                            "Failed to decode segment headers request"
                        );
                        continue;
                    }
                };
            trace!(?request, "Segment headers request");

            let response = if let Some((last_request, response)) = &last_request_response
                && last_request.segment_indices == request.segment_indices
            {
                response
            } else {
                match node_client
                    .segment_headers(request.segment_indices.clone())
                    .await
                {
                    Ok(segment_headers) => {
                        &last_request_response.insert((request, segment_headers)).1
                    }
                    Err(error) => {
                        warn!(
                            %error,
                            segment_indices = ?request.segment_indices,
                            "Failed to get segment headers"
                        );
                        continue;
                    }
                }
            };

            if let Some(reply_subject) = message.reply {
                if let Err(error) = nats_client
                    .publish(reply_subject, response.encode().into())
                    .await
                {
                    warn!(%error, "Failed to send farmer app info response");
                }
            }
        }

        Ok(())
    };

    // TODO: Smarter piece handling with requests for cached pieces being redirected directly to
    //  cache instances directly
    let piece_responder_fut = async {
        let mut subscription = nats_client
            .queue_subscribe(
                ClusterControllerPieceRequest::SUBJECT,
                "subspace.controller".to_string(),
            )
            .await
            .map_err(|error| anyhow!("Failed to subscribe to piece requests: {error}"))?;

        while let Some(message) = subscription.next().await {
            let request = match ClusterControllerPieceRequest::decode(&mut message.payload.as_ref())
            {
                Ok(request) => request,
                Err(error) => {
                    warn!(
                        %error,
                        message = %hex::encode(message.payload),
                        "Failed to decode piece request"
                    );
                    continue;
                }
            };
            trace!(?request, "Piece request");

            let maybe_piece: <ClusterControllerPieceRequest as GenericRequest>::Response =
                match piece_getter.get_piece(request.piece_index).await {
                    Ok(maybe_piece) => maybe_piece,
                    Err(error) => {
                        warn!(
                            %error,
                            piece_index = %request.piece_index,
                            "Failed to get piece"
                        );
                        continue;
                    }
                };

            if let Some(reply_subject) = message.reply {
                if let Err(error) = nats_client
                    .publish(reply_subject, maybe_piece.encode().into())
                    .await
                {
                    warn!(%error, "Failed to send farmer app info response");
                }
            }
        }

        Ok(())
    };

    select! {
        result = slot_info_broadcasting_fut.fuse() => {
            result
        },
        result = reward_signing_broadcasting_fut.fuse() => {
            result
        },
        result = archived_segment_headers_broadcasting_fut.fuse() => {
            result
        },
        result = solution_response_forwarding_fut.fuse() => {
            result
        },
        result = reward_signature_forwarding_fut.fuse() => {
            result
        },
        result = farmer_app_info_responder_fut.fuse() => {
            result
        },
        result = segment_headers_responder_fut.fuse() => {
            result
        },
        result = piece_responder_fut.fuse() => {
            result
        },
    }
}

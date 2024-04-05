use async_nats::{
    Client, HeaderMap, HeaderValue, PublishError, RequestError, RequestErrorKind, Subject,
    Subscriber, ToServerAddrs,
};
use derive_more::{Deref, DerefMut};
use futures::{Stream, StreamExt};
use parity_scale_codec::{Decode, Encode};
use std::any::type_name;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::warn;

/// Generic request with associated response
pub trait GenericRequest: Encode + Decode + Send + Sync + 'static {
    /// Request subject with optional `*` in place of application instance to receive the request
    const SUBJECT: &'static str;
    /// Response type that corresponds to this request
    type Response: Encode + Decode + Send + Sync + 'static;
}

/// Generic one-off notification
pub trait GenericNotification: Encode + Decode + Send + Sync + 'static {
    /// Request subject with optional `*` in place of application instance to receive the request
    const SUBSCRIPTION_SUBJECT: &'static str;
}

/// Generic broadcast message.
///
/// Broadcast messages are sent by an instance to (potentially) an instance-specific subject that
/// any other app can subscribe to. The same broadcast message can also originate from multiple
/// places and be de-duplicated using [`Self::deterministic_message_id`].
pub trait GenericBroadcast: Encode + Decode + Send + Sync + 'static {
    /// Subject that can be used for subscriptions with optional `*` in place of application
    /// instance sending broadcast
    const SUBSCRIPTION_SUBJECT: &'static str;

    /// Deterministic message ID that is used for de-duplicating messages broadcast by different
    /// instances
    fn deterministic_message_id(&self) -> Option<HeaderValue> {
        None
    }
}

#[derive(Debug, Deref, DerefMut)]
#[pin_project::pin_project]
pub struct SubscriberWrapper<Message> {
    #[pin]
    #[deref]
    #[deref_mut]
    subscriber: Subscriber,
    _phantom: PhantomData<Message>,
}

impl<Message> Stream for SubscriberWrapper<Message>
where
    Message: Decode,
{
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().subscriber.poll_next_unpin(cx) {
            Poll::Ready(Some(message)) => match Message::decode(&mut message.payload.as_ref()) {
                Ok(message) => Poll::Ready(Some(message)),
                Err(error) => {
                    warn!(
                        %error,
                        message_type = %type_name::<Message>(),
                        message = %hex::encode(message.payload),
                        "Failed to decode stream message"
                    );

                    Poll::Pending
                }
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// NATS client wrapper that can be used to interact with other Subspace-specific clients
#[derive(Debug, Clone, Deref)]
pub struct NatsClient {
    client: Client,
}

impl From<Client> for NatsClient {
    fn from(client: Client) -> Self {
        Self { client }
    }
}

impl NatsClient {
    /// Create new instance by connecting to specified addresses
    pub async fn new<A: ToServerAddrs>(addrs: A) -> Result<Self, async_nats::Error> {
        Ok(Self {
            client: async_nats::connect(addrs).await?,
        })
    }

    /// Make request and wait for response
    pub async fn request<Request>(
        &self,
        request: &Request,
        instance: Option<&str>,
    ) -> Result<Request::Response, RequestError>
    where
        Request: GenericRequest,
    {
        let subject = if let Some(instance) = instance {
            Subject::from(Request::SUBJECT.replace('*', instance))
        } else {
            Subject::from_static(Request::SUBJECT)
        };
        let message = self
            .client
            .request(subject.clone(), request.encode().into())
            .await?;

        let response =
            Request::Response::decode(&mut message.payload.as_ref()).map_err(|error| {
                warn!(
                    %subject,
                    %error,
                    response_type = %type_name::<Request::Response>(),
                    response = %hex::encode(message.payload),
                    "Response decoding failed"
                );

                RequestErrorKind::Other
            })?;

        Ok(response)
    }

    /// Make notification without waiting for response
    pub async fn notification<Notification>(
        &self,
        notification: &Notification,
        instance: Option<&str>,
    ) -> Result<(), PublishError>
    where
        Notification: GenericNotification,
    {
        let subject = if let Some(instance) = instance {
            Subject::from(Notification::SUBSCRIPTION_SUBJECT.replace('*', instance))
        } else {
            Subject::from_static(Notification::SUBSCRIPTION_SUBJECT)
        };

        self.client
            .publish(subject, notification.encode().into())
            .await
    }

    /// Send a broadcast message
    pub async fn broadcast<Broadcast>(
        &self,
        message: &Broadcast,
        instance: &str,
    ) -> Result<(), PublishError>
    where
        Broadcast: GenericBroadcast,
    {
        self.client
            .publish_with_headers(
                Broadcast::SUBSCRIPTION_SUBJECT.replace('*', instance),
                {
                    let mut headers = HeaderMap::new();
                    if let Some(message_id) = message.deterministic_message_id() {
                        headers.insert("Nats-Msg-Id", message_id);
                    }
                    headers
                },
                message.encode().into(),
            )
            .await
    }
}

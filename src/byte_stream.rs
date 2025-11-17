use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body::Frame;
use thiserror::Error;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Error, Debug, Clone)]
pub enum BodyStreamError {
    #[error("An error occurred while producing the stream data")]
    SourceError,

    #[error("The broadcast stream receiver lagged and lost {0} messages. The stream is now corrupt.")]
    Lagged(u64),

    #[error("Fail to get next chunk from soundcloud")]
    ChunkError,
}


pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BodyStreamError>> + Send>>;


type Broadcast = BroadcastStream<Result<Bytes, BodyStreamError>>;

pub struct BroadcastStreamBodyWrapper {
    stream: Broadcast,
}

impl BroadcastStreamBodyWrapper {
    /// Create a new `BroadcastStreamBodyWrapper`.
    pub fn new(stream: Broadcast) -> Self {
        Self { stream }
    }
}

impl http_body::Body for BroadcastStreamBodyWrapper {
    type Data = Bytes;
    type Error = BodyStreamError; // Use the more descriptive error type

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(outer_result)) => {
                match outer_result {
                    // Success case: we received the inner result from the broadcast channel
                    Ok(inner_result) => match inner_result {
                        // Success case: the inner result contains our data
                        Ok(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                        // Error case: the inner result is an error from the stream's source
                        Err(e) => Poll::Ready(Some(Err(e))),
                    },
                    // Error case: the broadcast channel itself returned an error
                    Err(broadcast_err) => match broadcast_err {
                        // This receiver lagged and lost messages. This is a fatal error
                        BroadcastStreamRecvError::Lagged(num_skipped) => {
                            Poll::Ready(Some(Err(BodyStreamError::Lagged(num_skipped))))
                        }
                    },
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}